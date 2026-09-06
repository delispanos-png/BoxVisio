/*
  BoxVisio BI Bridge for SoftOne Advanced JavaScript
  Version: 2026-09-06_cash-socash-account

  Purpose
  - Extract Sales, Purchases, Inventory, Cash, Balances, Expenses data directly from SoftOne tables.
  - Return API-friendly JSON records for BoxVisio BI ingestion.
  - No custom browser list dependency.
  - Safe live defaults: GetAll excludes heavy balance streams unless explicitly requested.
  - Read queries are decorated with NOLOCK hints where possible to reduce pressure on live SoftOne SQL.

  Intended use
  - Install in SoftOne Advanced JavaScript package (example: myWS).
  - Call via:
      /s1services/JS/myWS/GetSalesDocumentsForBI
      /s1services/JS/myWS/GetPurchaseDocumentsForBI
      /s1services/JS/myWS/GetInventoryDocumentsForBI
      /s1services/JS/myWS/GetItemMasterForBI
      /s1services/JS/myWS/GetCashTransactionsForBI
      /s1services/JS/myWS/GetSupplierBalancesForBI
      /s1services/JS/myWS/GetCustomerBalancesForBI
      /s1services/JS/myWS/GetOperatingExpensesForBI
      /s1services/JS/myWS/GetSupplierOrdersForBI
      /s1services/JS/myWS/GetAllForBI
*/

var BVBI_VERSION = "2026-09-06_cash-socash-account";
var _BVBI_COL_CACHE = {};

function _bv_is_array(v) {
  return Object.prototype.toString.call(v) === "[object Array]";
}

function _bv_pad2(n) {
  n = parseInt(n, 10);
  if (isNaN(n)) n = 0;
  return (n < 10 ? "0" : "") + String(n);
}

function _bv_now_iso() {
  var d = new Date();
  return (
    d.getFullYear() +
    "-" +
    _bv_pad2(d.getMonth() + 1) +
    "-" +
    _bv_pad2(d.getDate()) +
    "T" +
    _bv_pad2(d.getHours()) +
    ":" +
    _bv_pad2(d.getMinutes()) +
    ":" +
    _bv_pad2(d.getSeconds())
  );
}

function _bv_text(v, fallbackValue) {
  if (v === null || v === undefined) return fallbackValue === undefined ? "" : fallbackValue;
  return String(v);
}

function _bv_num(v, fallbackValue) {
  var x = parseFloat(v);
  if (isNaN(x)) return fallbackValue === undefined ? 0 : fallbackValue;
  return x;
}

function _bv_bool(v, fallbackValue) {
  if (v === null || v === undefined) return fallbackValue === undefined ? false : fallbackValue;
  if (typeof v === "boolean") return v;
  var s = String(v).toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "y" || s === "on";
}

function _bv_norm_date(v) {
  var s = _bv_text(v, "").replace(/\//g, "-").trim();
  if (s === "") return "";
  if (s.length >= 10) s = s.substr(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return "";
  var y = parseInt(s.substr(0, 4), 10);
  var m = parseInt(s.substr(5, 2), 10);
  var d = parseInt(s.substr(8, 2), 10);
  if (isNaN(y) || isNaN(m) || isNaN(d)) return "";
  var dt = new Date(y, m - 1, d);
  if (dt.getFullYear() !== y || dt.getMonth() + 1 !== m || dt.getDate() !== d) return "";
  return s;
}

function _bv_sql_quote(v) {
  return "'" + _bv_text(v, "").replace(/'/g, "''") + "'";
}

function _bv_int(v, fallbackValue, minValue, maxValue) {
  var n = parseInt(v, 10);
  if (isNaN(n)) n = fallbackValue;
  if (typeof minValue === "number" && n < minValue) n = minValue;
  if (typeof maxValue === "number" && n > maxValue) n = maxValue;
  return n;
}

function _bv_top_clause(limitValue) {
  var n = _bv_int(limitValue, 0, 0, 200000);
  return n > 0 ? "TOP " + n + " " : "";
}

function _bv_balance_top_clause(cfg) {
  if (cfg && cfg.allowFullBalanceSync) return _bv_top_clause(cfg.limit);
  var n = _bv_int(cfg && cfg.limit, 0, 0, 5000);
  if (n <= 0) n = _bv_int(cfg && cfg.balanceLimit, 500, 100, 5000);
  return _bv_top_clause(n);
}

function _bv_sqlserver_read_hints(sql) {
  var out = _bv_text(sql, "");
  var replacements = [
    ["FROM FINDOC F ", "FROM FINDOC F WITH (NOLOCK) "],
    ["FROM FINDOC ORIGF ", "FROM FINDOC ORIGF WITH (NOLOCK) "],
    ["INNER JOIN MTRLINES L ON", "INNER JOIN MTRLINES L WITH (NOLOCK) ON"],
    ["LEFT JOIN MTRDOC MD ON", "LEFT JOIN MTRDOC MD WITH (NOLOCK) ON"],
    ["LEFT JOIN MTRDOC OMD ON", "LEFT JOIN MTRDOC OMD WITH (NOLOCK) ON"],
    ["LEFT JOIN WHOUSE W ON", "LEFT JOIN WHOUSE W WITH (NOLOCK) ON"],
    ["LEFT JOIN TRDR C ON", "LEFT JOIN TRDR C WITH (NOLOCK) ON"],
    ["LEFT JOIN TRDR S ON", "LEFT JOIN TRDR S WITH (NOLOCK) ON"],
    ["LEFT JOIN TRDR T ON", "LEFT JOIN TRDR T WITH (NOLOCK) ON"],
    ["LEFT JOIN TRDR CA ON", "LEFT JOIN TRDR CA WITH (NOLOCK) ON"],
    ["LEFT JOIN MTRL I ON", "LEFT JOIN MTRL I WITH (NOLOCK) ON"],
    ["LEFT JOIN MTRGROUP MG ON", "LEFT JOIN MTRGROUP MG WITH (NOLOCK) ON"],
    ["LEFT JOIN SALDOC SD ON", "LEFT JOIN SALDOC SD WITH (NOLOCK) ON"],
    ["LEFT JOIN TRDBRANCH CB ON", "LEFT JOIN TRDBRANCH CB WITH (NOLOCK) ON"],
    ["LEFT JOIN TRDBRANCH OCB ON", "LEFT JOIN TRDBRANCH OCB WITH (NOLOCK) ON"],
    ["FROM EXPANAL EA ", "FROM EXPANAL EA WITH (NOLOCK) "],
    ["FROM EXPANAL EA2 ", "FROM EXPANAL EA2 WITH (NOLOCK) "],
    ["LEFT JOIN EXPN EN ON", "LEFT JOIN EXPN EN WITH (NOLOCK) ON"],
    ["LEFT JOIN EXPN EN2 ON", "LEFT JOIN EXPN EN2 WITH (NOLOCK) ON"],
    ["FROM MTRL I ", "FROM MTRL I WITH (NOLOCK) "],
    ["FROM MTRSUBSTITUTE MS ", "FROM MTRSUBSTITUTE MS WITH (NOLOCK) "],
    ["LEFT JOIN VAT VT ON", "LEFT JOIN VAT VT WITH (NOLOCK) ON"],
    ["LEFT JOIN MTRMARK MK ON", "LEFT JOIN MTRMARK MK WITH (NOLOCK) ON"],
    ["LEFT JOIN MTRMANFCTR MF ON", "LEFT JOIN MTRMANFCTR MF WITH (NOLOCK) ON"],
    ["LEFT JOIN MTREXTRA IX ON", "LEFT JOIN MTREXTRA IX WITH (NOLOCK) ON"],
    ["LEFT JOIN ITEEXTRA IX ON", "LEFT JOIN ITEEXTRA IX WITH (NOLOCK) ON"],
    ["LEFT JOIN MTREXTRA RX ON", "LEFT JOIN MTREXTRA RX WITH (NOLOCK) ON"],
    ["LEFT JOIN ITEEXTRA RX ON", "LEFT JOIN ITEEXTRA RX WITH (NOLOCK) ON"],
    ["LEFT JOIN UTBL04 U4 ON", "LEFT JOIN UTBL04 U4 WITH (NOLOCK) ON"],
    ["LEFT JOIN CCC88POCAT1 PC1 ON", "LEFT JOIN CCC88POCAT1 PC1 WITH (NOLOCK) ON"],
    ["LEFT JOIN CCC88POCAT2 PC2 ON", "LEFT JOIN CCC88POCAT2 PC2 WITH (NOLOCK) ON"],
    ["LEFT JOIN CCC88POCAT3 PC3 ON", "LEFT JOIN CCC88POCAT3 PC3 WITH (NOLOCK) ON"],
    ["LEFT JOIN MTRPCATEGORY CG ON", "LEFT JOIN MTRPCATEGORY CG WITH (NOLOCK) ON"],
    ["LEFT JOIN ACCOUNT AC ON", "LEFT JOIN ACCOUNT AC WITH (NOLOCK) ON"],
    ["FROM TRDFLINES TL ", "FROM TRDFLINES TL WITH (NOLOCK) "],
    ["INNER JOIN TRDFLINES TL ON", "INNER JOIN TRDFLINES TL WITH (NOLOCK) ON"],
    ["LEFT JOIN TRDFLINES TL ON", "LEFT JOIN TRDFLINES TL WITH (NOLOCK) ON"],
    ["FROM CCC88ECHANNEL E ", "FROM CCC88ECHANNEL E WITH (NOLOCK) "],
    ["FROM PAYMENT OP ", "FROM PAYMENT OP WITH (NOLOCK) "],
    ["FROM PAYMENT P ", "FROM PAYMENT P WITH (NOLOCK) "]
  ];
  var i;
  for (i = 0; i < replacements.length; i++) {
    out = out.split(replacements[i][0]).join(replacements[i][1]);
  }
  return out;
}

function _bv_ds_first(ds) {
  try {
    ds.FIRST;
    return;
  } catch (e1) {}
  try {
    ds.FIRST();
  } catch (e2) {}
}

function _bv_ds_next(ds) {
  try {
    ds.NEXT;
    return;
  } catch (e1) {}
  try {
    ds.NEXT();
  } catch (e2) {}
}

function _bv_ds_eof(ds) {
  try {
    return ds.EOF;
  } catch (e1) {}
  try {
    return ds.EOF();
  } catch (e2) {}
  return true;
}

function _bv_ds_count(ds) {
  try {
    return parseInt(ds.RECORDCOUNT, 10) || 0;
  } catch (e1) {}
  try {
    return parseInt(ds.RECORDCOUNT(), 10) || 0;
  } catch (e2) {}
  return 0;
}

function _bv_field(ds, fieldName, fallbackValue) {
  var names = [_bv_text(fieldName, ""), _bv_text(fieldName, "").toUpperCase(), _bv_text(fieldName, "").toLowerCase()];
  var i;
  for (i = 0; i < names.length; i++) {
    try {
      if (ds[names[i]] !== null && ds[names[i]] !== undefined) return ds[names[i]];
    } catch (e1) {}
  }
  return fallbackValue;
}

function _bv_dataset_records(ds, mapper) {
  var out = [];
  if (!ds) return out;
  if (_bv_ds_count(ds) <= 0) return out;
  _bv_ds_first(ds);
  while (!_bv_ds_eof(ds)) {
    out.push(mapper(ds));
    _bv_ds_next(ds);
  }
  return out;
}

function _bv_get_columns(tableName) {
  var key = _bv_text(tableName, "").toUpperCase();
  if (_BVBI_COL_CACHE[key]) return _BVBI_COL_CACHE[key];

  var cols = {};
  var sql =
    "SELECT COLUMN_NAME AS COL " +
    "FROM INFORMATION_SCHEMA.COLUMNS " +
    "WHERE TABLE_NAME=" +
    _bv_sql_quote(key);
  var ds = X.GETSQLDATASET(sql, null);
  if (_bv_ds_count(ds) > 0) {
    _bv_ds_first(ds);
    while (!_bv_ds_eof(ds)) {
      cols[_bv_text(_bv_field(ds, "COL", ""), "").toUpperCase()] = true;
      _bv_ds_next(ds);
    }
  }
  _BVBI_COL_CACHE[key] = cols;
  return cols;
}

function _bv_has_column(tableName, colName) {
  var cols = _bv_get_columns(tableName);
  return !!cols[_bv_text(colName, "").toUpperCase()];
}

function _bv_table_exists(tableName) {
  var cols = _bv_get_columns(tableName);
  var k;
  for (k in cols) {
    if (cols.hasOwnProperty(k)) return true;
  }
  return false;
}

function _bv_pick_column(tableName, candidates) {
  var i;
  for (i = 0; i < candidates.length; i++) {
    if (_bv_has_column(tableName, candidates[i])) return candidates[i];
  }
  return "";
}

function _bv_col_expr(aliasName, tableName, candidates, fallbackExpr) {
  var c = _bv_pick_column(tableName, candidates);
  if (c !== "") return aliasName + "." + c;
  return fallbackExpr;
}

function _bv_item_manual_order_info(itemAlias) {
  var extraTable = "";
  var ixMtrlExpr;
  var ixCompanyExpr;
  var ixUtblexpr;
  var ixStatusExpr;
  var u4NameExpr;
  var u4CodeExpr;
  var u5NameExpr;
  var u5CodeExpr;
  var info = { expr: "''", commercialStatusExpr: "''", joinSql: "" };
  if (_bv_table_exists("ITEEXTRA")) {
    extraTable = "ITEEXTRA";
  } else if (_bv_table_exists("MTREXTRA")) {
    extraTable = "MTREXTRA";
  }
  if (extraTable === "") return info;

  ixMtrlExpr = _bv_col_expr("IX", extraTable, ["MTRL"], "NULL");
  ixCompanyExpr = _bv_col_expr("IX", extraTable, ["COMPANY"], itemAlias + ".COMPANY");
  ixUtblexpr = _bv_col_expr("IX", extraTable, ["UTBL04"], "NULL");
  ixStatusExpr = _bv_col_expr("IX", extraTable, ["UTBL05"], "NULL");
  info.joinSql = "LEFT JOIN " + extraTable + " IX ON " + ixMtrlExpr + " = " + itemAlias + ".MTRL AND " + ixCompanyExpr + " = " + itemAlias + ".COMPANY ";
  info.expr = "NULLIF(CAST(" + ixUtblexpr + " AS VARCHAR(128)), '0')";
  info.commercialStatusExpr = "NULLIF(CAST(" + ixStatusExpr + " AS VARCHAR(128)), '0')";
  if (_bv_table_exists("UTBL04")) {
    u4NameExpr = _bv_col_expr("U4", "UTBL04", ["NAME"], "''");
    u4CodeExpr = _bv_col_expr("U4", "UTBL04", ["CODE"], "''");
    info.joinSql +=
      "LEFT JOIN UTBL04 U4 ON U4.UTBL04 = " +
      ixUtblexpr +
      " AND U4.COMPANY = " +
      ixCompanyExpr +
      " AND U4.SODTYPE = " +
      itemAlias +
      ".SODTYPE ";
    info.expr =
      "COALESCE(NULLIF(" +
      u4NameExpr +
      ", ''), NULLIF(" +
      u4CodeExpr +
      ", ''), " +
      info.expr +
      ", '')";
  }
  if (_bv_table_exists("UTBL05")) {
    u5NameExpr = _bv_col_expr("U5", "UTBL05", ["NAME"], "''");
    u5CodeExpr = _bv_col_expr("U5", "UTBL05", ["CODE"], "''");
    info.joinSql +=
      "LEFT JOIN UTBL05 U5 ON U5.UTBL05 = " +
      ixStatusExpr +
      " AND U5.COMPANY = " +
      ixCompanyExpr +
      " AND U5.SODTYPE = " +
      itemAlias +
      ".SODTYPE ";
    info.commercialStatusExpr =
      "COALESCE(NULLIF(" +
      u5NameExpr +
      ", ''), NULLIF(" +
      u5CodeExpr +
      ", ''), " +
      info.commercialStatusExpr +
      ", '')";
  }
  return info;
}

function _bv_item_replenishment_info(itemAlias) {
  var extraTable = "";
  var ixMtrlExpr;
  var ixCompanyExpr;
  var vendorMoqJoinSql = "";
  var info = {
    joinSql: "",
    status1Expr: "''",
    status2Expr: "''",
    minStockExpr: "1",
    replMoqExpr: "1",
    vendorMoqExpr: "1",
    purchasePriceExpr: _bv_col_expr(itemAlias, "MTRL", ["LASTPURPRICE", "PURPRICE", "PRICEP", "PRICEW"], "NULL")
  };
  if (_bv_table_exists("MTREXTRA")) {
    extraTable = "MTREXTRA";
  } else if (_bv_table_exists("ITEEXTRA")) {
    extraTable = "ITEEXTRA";
  }
  if (extraTable === "") {
    info.status1Expr = _bv_col_expr(itemAlias, "MTRL", ["REPLSTATUS1", "FNRSTATUS1", "STATUS1"], "''");
    info.status2Expr = _bv_col_expr(itemAlias, "MTRL", ["REPLSTATUS2", "FNRSTATUS2", "STATUS2"], "''");
    info.minStockExpr = "1";
    info.replMoqExpr = "1";
    info.vendorMoqExpr = "1";
  }
  if (extraTable !== "") {
    ixMtrlExpr = _bv_col_expr("RX", extraTable, ["MTRL"], "NULL");
    ixCompanyExpr = _bv_col_expr("RX", extraTable, ["COMPANY"], itemAlias + ".COMPANY");
    info.joinSql = "LEFT JOIN " + extraTable + " RX ON " + ixMtrlExpr + " = " + itemAlias + ".MTRL AND " + ixCompanyExpr + " = " + itemAlias + ".COMPANY ";
    info.status1Expr =
      "COALESCE(NULLIF(CAST(" +
      _bv_col_expr("RX", extraTable, ["REPLSTATUS1", "FNRSTATUS1", "STATUS1", "STATUS_1", "UTBL01"], "''") +
      " AS VARCHAR(64)), ''), NULLIF(CAST(" +
      _bv_col_expr(itemAlias, "MTRL", ["REPLSTATUS1", "FNRSTATUS1", "STATUS1"], "''") +
      " AS VARCHAR(64)), ''), '')";
    info.status2Expr =
      "COALESCE(NULLIF(CAST(" +
      _bv_col_expr("RX", extraTable, ["REPLSTATUS2", "FNRSTATUS2", "STATUS2", "STATUS_2", "UTBL02"], "''") +
      " AS VARCHAR(128)), ''), NULLIF(CAST(" +
      _bv_col_expr(itemAlias, "MTRL", ["REPLSTATUS2", "FNRSTATUS2", "STATUS2"], "''") +
      " AS VARCHAR(128)), ''), '')";
    info.minStockExpr = "1";
    info.replMoqExpr = "1";
    info.purchasePriceExpr =
      "COALESCE(" +
      _bv_col_expr("RX", extraTable, ["PURCHASEPRICE", "PURCHASE_PRICE", "LASTPURPRICE", "FINALPURCHASEPRICE", "NUM04"], "NULL") +
      ", " +
      info.purchasePriceExpr +
      ")";
  }
  if (_bv_table_exists("MTRSUPCODE") && _bv_has_column("MTRSUPCODE", "CCC88MOQ")) {
    vendorMoqJoinSql =
      "OUTER APPLY (SELECT MIN(NULLIF(TRY_CAST(MSC.CCC88MOQ AS FLOAT), 0)) AS VENDOR_MOQ " +
      "FROM MTRSUPCODE MSC WHERE MSC.MTRL = " +
      itemAlias +
      ".MTRL AND MSC.COMPANY = " +
      itemAlias +
      ".COMPANY) VMQ ";
    info.joinSql += vendorMoqJoinSql;
    info.vendorMoqExpr = "COALESCE(VMQ.VENDOR_MOQ, 1)";
  }
  return info;
}

function _bv_parse_source_codes(raw, defaultsCsv) {
  var list = [];
  var i;
  if (_bv_is_array(raw)) {
    for (i = 0; i < raw.length; i++) list.push(_bv_text(raw[i], ""));
  } else {
    var txt = _bv_text(raw, "");
    if (txt === "") txt = defaultsCsv;
    var arr = txt.split(",");
    for (i = 0; i < arr.length; i++) list.push(arr[i]);
  }

  var clean = [];
  var seen = {};
  for (i = 0; i < list.length; i++) {
    var x = _bv_text(list[i], "").replace(/\s+/g, "");
    if (/^\d+$/.test(x) && !seen[x]) {
      seen[x] = true;
      clean.push(x);
    }
  }

  if (clean.length > 200) clean = clean.slice(0, 200);
  if (clean.length === 0) return defaultsCsv;
  return clean.join(",");
}

function _bv_require_client(obj) {
  if (!obj || _bv_text(obj.clientID, "") === "") {
    throw "Missing clientID. Call this function through authenticated SoftOne web services.";
  }
}

function _bv_resolve_request(obj) {
  var r = {};
  r.company = _bv_int(obj && obj.company, _bv_int(X.SYS.COMPANY, 0, 1, 99999999), 1, 99999999);
  // default limit=0 => no SQL TOP cap for document streams (balances are capped unless explicitly allowed)
  r.limit = _bv_int(obj && obj.limit, 0, 0, 200000);
  r.balanceLimit = _bv_int(obj && obj.balanceLimit, 500, 100, 5000);
  r.allowFullBalanceSync = _bv_bool(obj && obj.allowFullBalanceSync, false);
  r.fromDate = _bv_norm_date(obj && obj.fromDate);
  r.toDate = _bv_norm_date(obj && obj.toDate);
  r.debug = _bv_bool(obj && obj.debug, false);

  r.includeSales = _bv_bool(obj && obj.includeSales, true);
  r.includePurchases = _bv_bool(obj && obj.includePurchases, true);
  r.includeInventory = _bv_bool(obj && obj.includeInventory, true);
  r.includeItemMaster = _bv_bool(obj && obj.includeItemMaster, true);
  r.includeCashTransactions = _bv_bool(obj && (obj.includeCashTransactions !== undefined ? obj.includeCashTransactions : obj.includeCash), true);
  r.includeSupplierBalances = _bv_bool(obj && obj.includeSupplierBalances, false);
  r.includeCustomerBalances = _bv_bool(obj && obj.includeCustomerBalances, false);
  r.includeOperatingExpenses = _bv_bool(obj && obj.includeOperatingExpenses, true);
  r.includeSupplierOrders = _bv_bool(obj && obj.includeSupplierOrders, false);

  r.salesSourceCodes = _bv_parse_source_codes(obj && obj.salesSourceCodes, "1351,11351");
  r.purchaseSourceCodes = _bv_parse_source_codes(obj && obj.purchaseSourceCodes, "1251,1253");
  r.inventorySourceCodes = _bv_parse_source_codes(obj && obj.inventorySourceCodes, "1151");
  r.inventoryItemSoType = _bv_int(obj && obj.inventoryItemSoType, 51, 1, 99999999);
  r.cashSourceCodes = _bv_parse_source_codes(obj && obj.cashSourceCodes, "1261,1281,1381,1412,1413,1414,1415,1416,1481");
  r.supplierBalanceSourceCodes = _bv_parse_source_codes(
    obj && obj.supplierBalanceSourceCodes,
    "1251,1253,1261,1281,1412,1416,1653"
  );
  r.customerBalanceSourceCodes = _bv_parse_source_codes(
    obj && obj.customerBalanceSourceCodes,
    "1351,1381,1413"
  );
  r.expenseSourceCodes = _bv_parse_source_codes(obj && obj.expenseSourceCodes, "1261,1653");

  return r;
}

function _bv_findoc_common_exprs() {
  return {
    findoc: _bv_col_expr("F", "FINDOC", ["FINDOC"], "F.FINDOC"),
    company: _bv_col_expr("F", "FINDOC", ["COMPANY"], "0"),
    trnDate: _bv_col_expr("F", "FINDOC", ["TRNDATE"], "F.TRNDATE"),
    updDate: _bv_col_expr("F", "FINDOC", ["UPDDATE", "UPDATED", "LASTUPDATE", "TRNDATE"], "F.TRNDATE"),
    dueDate: _bv_col_expr("F", "FINDOC", ["LPAYDATE", "DUEDATE", "TRNDATE"], "F.TRNDATE"),
    branch: _bv_col_expr("F", "FINDOC", ["BRANCH"], "0"),
    series: _bv_col_expr("F", "FINDOC", ["SERIES"], "0"),
    finCode: _bv_col_expr("F", "FINDOC", ["FINCODE", "TRNNO", "DOCNUM"], "F.FINDOC"),
    trdr: _bv_col_expr("F", "FINDOC", ["TRDR"], "0"),
    sosource: _bv_col_expr("F", "FINDOC", ["SOSOURCE"], "0"),
    soredir: _bv_col_expr("F", "FINDOC", ["SOREDIR"], "0"),
    sodtype: _bv_col_expr("F", "FINDOC", ["SODTYPE"], "0"),
    tfprms: _bv_col_expr("F", "FINDOC", ["TFPRMS"], "0"),
    sumAmount: _bv_col_expr("F", "FINDOC", ["SUMAMNT", "NETAMNT", "TRNVAL", "VAL", "AMNT"], "0"),
    taxAmount: _bv_col_expr("F", "FINDOC", ["VATAMNT", "TAXAMNT", "FPAAMNT"], "0"),
    comments: _bv_col_expr("F", "FINDOC", ["COMMENTS", "REMARKS", "LCOMMENTS", "NOTES"], "''"),
    account: _bv_col_expr("F", "FINDOC", ["ACNT", "ACCOUNT", "BANKACCOUNT", "CASHACCOUNT"], "0")
  };
}

function _bv_series_info_expr(common) {
  var out = {
    joinSql: "",
    seriesNameExpr: "CAST(ISNULL(" + common.series + ",0) AS VARCHAR(255))"
  };

  if (!_bv_table_exists("SERIES")) return out;

  var srSeries = _bv_col_expr("SR", "SERIES", ["SERIES"], "0");
  var srCompany = _bv_col_expr("SR", "SERIES", ["COMPANY"], "0");
  var srName = _bv_col_expr("SR", "SERIES", ["NAME", "DESCR", "TITLE"], "''");

  out.joinSql = " LEFT JOIN SERIES SR ON " + srSeries + "=" + common.series + " AND " + srCompany + "=F.COMPANY";
  if (_bv_has_column("SERIES", "SOSOURCE")) {
    var srSource = _bv_col_expr("SR", "SERIES", ["SOSOURCE"], "0");
    out.joinSql += " AND " + srSource + "=" + common.sosource;
  }
  out.seriesNameExpr = "CAST(ISNULL(" + srName + ", " + common.series + ") AS VARCHAR(255))";
  return out;
}

function _bv_branch_info_expr(common) {
  var out = {
    joinSql: "",
    branchNameExpr: "CAST(ISNULL(" + common.branch + ", '') AS VARCHAR(255))"
  };

  if (!_bv_table_exists("BRANCH")) return out;

  var brBranch = _bv_col_expr("BR", "BRANCH", ["BRANCH", "BRANCHID", "CODE"], common.branch);
  var brCompany = _bv_col_expr("BR", "BRANCH", ["COMPANY"], common.company);
  var brName = _bv_col_expr("BR", "BRANCH", ["NAME", "DESCR", "TITLE"], "''");

  out.joinSql = " LEFT JOIN BRANCH BR ON " + brBranch + "=" + common.branch + " AND " + brCompany + "=" + common.company;
  out.branchNameExpr = "CAST(ISNULL(" + brName + ", " + common.branch + ") AS VARCHAR(255))";
  return out;
}

function _bv_sales_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var seriesInfo = _bv_series_info_expr(c);
  var branchInfo = _bv_branch_info_expr(c);

  var lLineId = _bv_col_expr("L", "MTRLINES", ["MTRLINES", "LINENUM"], "0");
  var lFindocs = _bv_col_expr("L", "MTRLINES", ["FINDOCS"], "0");
  var lMtrl = _bv_col_expr("L", "MTRLINES", ["MTRL"], "0");
  var iName = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var docChannel = _bv_col_expr("F", "FINDOC", ["CCC88ECHANNEL"], "0");
  var originChannel = _bv_col_expr("ORIG", "FINDOC", ["CCC88ECHANNEL"], "0");
  var docOrderNo = _bv_col_expr("F", "FINDOC", ["CCC88EORDERNO"], "''");
  var originOrderNo = _bv_col_expr("ORIG", "FINDOC", ["CCC88EORDERNO"], "''");
  var channelName = _bv_col_expr("EC", "CCC88ECHANNEL", ["NAME"], "''");
  var paymentCode = _bv_col_expr("PM", "PAYMENT", ["CODE"], "''");
  var paymentName = _bv_col_expr("PM", "PAYMENT", ["NAME"], "''");
  var originPaymentCode = _bv_col_expr("OPM", "PAYMENT", ["CODE"], "''");
  var originPaymentName = _bv_col_expr("OPM", "PAYMENT", ["NAME"], "''");
  var saldocOrderNo = _bv_col_expr("SD", "SALDOC", ["CCC88EORDERNO"], "''");
  var docPayment = _bv_col_expr("F", "FINDOC", ["PAYMENT"], "0");
  var originPayment = _bv_col_expr("ORIG", "FINDOC", ["PAYMENT"], "0");
  var originSodtype = _bv_col_expr("ORIG", "FINDOC", ["SODTYPE"], c.sodtype);
  var docCustomerBranchId = _bv_col_expr("F", "FINDOC", ["TRDBRANCH"], "0");
  var originCustomerBranchId = _bv_col_expr("ORIG", "FINDOC", ["TRDBRANCH"], "0");
  var docCarrierTrdr = _bv_col_expr("F", "FINDOC", ["TRNTRDR", "TRDRSHIP", "CARRIER"], "0");
  var docChannelResolved =
    "COALESCE(NULLIF(CAST(ISNULL(" +
    docChannel +
    ",0) AS VARCHAR(64)),'0'), NULLIF(CAST(ISNULL(" +
    originChannel +
    ",0) AS VARCHAR(64)),'0'), CASE WHEN COALESCE(NULLIF(LTRIM(RTRIM(ISNULL(" +
    docOrderNo +
    ",''))),''), NULLIF(LTRIM(RTRIM(ISNULL(" +
    originOrderNo +
    ",''))),''), NULLIF(LTRIM(RTRIM(ISNULL(" +
    saldocOrderNo +
    ",''))),'')) IS NOT NULL THEN '1' ELSE NULL END)";
  var eshopCode = "COALESCE(NULLIF(" + docOrderNo + ",''), NULLIF(" + originOrderNo + ",''), NULLIF(" + saldocOrderNo + ",''), '')";
  var originShippingMethod = _bv_col_expr("ORIG", "FINDOC", ["SHIPPINGMETHOD", "SHIPMENTMETHOD", "DISPATCHMETHOD", "SENDMETHOD", "SHIPPING", "SHIPMENT", "DELIVMODE", "SHIPMENTMODE"], "''");
  var docShippingMethod = _bv_col_expr("F", "FINDOC", ["SHIPPINGMETHOD", "SHIPMENTMETHOD", "DISPATCHMETHOD", "SENDMETHOD", "SHIPPING", "SHIPMENT", "DELIVMODE", "SHIPMENTMODE"], "''");
  var shippingMethod = "COALESCE(NULLIF(" + originShippingMethod + ",''), NULLIF(" + _bv_col_expr("SD", "SALDOC", ["SHIPMENT"], "''") + ",''), NULLIF(" + docShippingMethod + ",''), '')";
  var originComments = _bv_col_expr("ORIG", "FINDOC", ["COMMENTS"], "''");
  var originOrderComments = _bv_col_expr("ORIG", "FINDOC", ["CCC88EORDERCOM"], "''");
  var reasonExpr = "COALESCE(NULLIF(" + c.comments + ",''), NULLIF(" + originComments + ",''), NULLIF(" + originOrderComments + ",''), '')";
  var customerBranch = _bv_col_expr("CB", "TRDBRANCH", ["NAME", "DESCRIPTION", "TITLE"], "''");
  var originCustomerBranch = _bv_col_expr("OCB", "TRDBRANCH", ["NAME", "DESCRIPTION", "TITLE"], "''");
  var deliveryAddress = "COALESCE(NULLIF(" + _bv_col_expr("OMD", "MTRDOC", ["SHIPPINGADDR"], "''") + ",''), NULLIF(" + _bv_col_expr("MD", "MTRDOC", ["SHIPPINGADDR"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["SHIPADRESS", "SHIPADDRESS", "DELIVADDRESS", "DELIVERYADDRESS"], "''") + ",''), '')";
  var deliveryZip = "COALESCE(NULLIF(" + _bv_col_expr("OMD", "MTRDOC", ["SHPZIP"], "''") + ",''), NULLIF(" + _bv_col_expr("MD", "MTRDOC", ["SHPZIP"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["SHIPZIP", "DELIVZIP", "DELIVERYZIP"], "''") + ",''), '')";
  var deliveryCity = "COALESCE(NULLIF(" + _bv_col_expr("OMD", "MTRDOC", ["SHPCITY"], "''") + ",''), NULLIF(" + _bv_col_expr("MD", "MTRDOC", ["SHPCITY"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["SHIPCITY", "DELIVCITY", "DELIVERYCITY"], "''") + ",''), '')";
  var deliveryArea = "COALESCE(NULLIF(" + _bv_col_expr("OMD", "MTRDOC", ["SHPDISTRICT"], "''") + ",''), NULLIF(" + _bv_col_expr("MD", "MTRDOC", ["SHPDISTRICT"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["SHIPAREA", "DELIVAREA", "DELIVERYAREA"], "''") + ",''), '')";
  var movementTypeName = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["MTRDOCMOVNAME", "MOVEMENTTYPE", "MOVTYPE"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["MTRDOCMOVNAME", "MOVEMENTTYPE", "MOVTYPE"], "''") + ",''), '')";
  var originMarketplaceCourier = _bv_col_expr("ORIG", "FINDOC", ["CCC88EMRKTCOUR"], "''");
  var docMarketplaceCourier = _bv_col_expr("F", "FINDOC", ["CCC88EMRKTCOUR"], "''");
  var carrierName = "COALESCE(NULLIF(" + originMarketplaceCourier + ",''), NULLIF(" + docMarketplaceCourier + ",''), NULLIF(" + _bv_col_expr("SD", "SALDOC", ["SHIPMENT"], "''") + ",''), NULLIF(" + _bv_col_expr("CA", "TRDR", ["NAME"], "''") + ",''), '')";
  var transportMedium = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["SHIPMENTMODE", "TRANSPORTMEANS", "VEHICLETYPE", "CCCVEHICLETYPE"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["SHIPMENTMODE", "TRANSPORTMEANS", "VEHICLETYPE", "CCCVEHICLETYPE"], "''") + ",''), '')";
  var transportNo = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["VEHICLENO", "TRANSPORTNO", "PLATENO", "CARNO"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["VEHICLENO", "TRANSPORTNO", "PLATENO", "CARNO"], "''") + ",''), '')";
  var routeName = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["ROUTENAME", "ITINERARY", "ROUTE"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["ROUTENAME", "ITINERARY", "ROUTE"], "''") + ",''), '')";
  var loadingDate = "COALESCE(" + _bv_col_expr("ORIG", "FINDOC", ["LOADINGDATE", "LOADDATE"], "NULL") + ", " + _bv_col_expr("SD", "SALDOC", ["BGDOCDATE1"], "NULL") + ", " + _bv_col_expr("F", "FINDOC", ["LOADINGDATE", "LOADDATE"], "NULL") + ", " + c.trnDate + ")";
  var deliveryDate = "COALESCE(" + _bv_col_expr("ORIG", "FINDOC", ["DELIVERYDATE", "SHIPDATE"], "NULL") + ", " + _bv_col_expr("F", "FINDOC", ["DELIVERYDATE", "SHIPDATE"], "NULL") + ", " + c.trnDate + ")";
  var originVoucher = _bv_col_expr("ORIG", "FINDOC", ["CCC88EVOUCHER"], "''");
  var docVoucher = _bv_col_expr("F", "FINDOC", ["CCC88EVOUCHER"], "''");
  var voucherNo = "COALESCE(NULLIF(" + originVoucher + ",''), NULLIF(" + docVoucher + ",''), '')";
  var voucherUrl = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["CCC88EVCHURL"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["CCC88EVCHURL"], "''") + ",''), '')";
  var lockerId = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["CCC88ELOCKERID"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["CCC88ELOCKERID"], "''") + ",''), '')";
  var orderComments = "COALESCE(NULLIF(" + originOrderComments + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["CCC88EORDERCOM"], "''") + ",''), '')";
  var shippingComments = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["CCC88ESHIPCOM"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["CCC88ESHIPCOM"], "''") + ",''), '')";
  var giftComments = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["CCC88EGIFTCOM"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["CCC88EGIFTCOM"], "''") + ",''), '')";
  var marketplaceInternalId = "COALESCE(NULLIF(" + _bv_col_expr("ORIG", "FINDOC", ["CCC88ELOGIKAID"], "''") + ",''), NULLIF(" + _bv_col_expr("F", "FINDOC", ["CCC88ELOGIKAID"], "''") + ",''), '')";
  var sourceCreatedAt = "COALESCE(" + _bv_col_expr("F", "FINDOC", ["INSDATE", "CREATED", "CREATEDAT", "INSERTDATE"], "NULL") + ", " + c.updDate + ")";
  var isCancel = _bv_col_expr("F", "FINDOC", ["ISCANCEL"], "0");
  var finStates = _bv_col_expr("F", "FINDOC", ["FINSTATES"], "0");
  var itemBrand = _bv_col_expr("I", "MTRL", ["MTRMARK"], "0");
  var brandName = _bv_col_expr("MK", "MTRMARK", ["NAME"], "''");
  var itemGroup = _bv_col_expr("I", "MTRL", ["MTRGROUP"], "0");
  var groupName = _bv_col_expr("MG", "MTRGROUP", ["NAME"], "''");
  var manualOrderInfo = _bv_item_manual_order_info("I");
  var manualOrderCategoryExpr = manualOrderInfo.expr;
  var commercialStatusExpr = manualOrderInfo.commercialStatusExpr;
  var itemExtraJoinSql = manualOrderInfo.joinSql;
  var replInfo = _bv_item_replenishment_info("I");
  var itemReplJoinSql = replInfo.joinSql;
  var shippingExpenseValueExpr = "0";
  var shippingExpenseDescriptionExpr = "''";
  var chargeRevenueNetValueExpr = "0";
  var chargeRevenueVatValueExpr = "0";
  var chargeRevenueGrossValueExpr = "0";
  var chargeRevenueDescriptionExpr = "''";
  var chargeRevenueLinesJsonExpr = "'[]'";
  var shippingChargeNetValueExpr = "0";
  var shippingChargeVatValueExpr = "0";
  var shippingChargeGrossValueExpr = "0";
  var codChargeNetValueExpr = "0";
  var codChargeVatValueExpr = "0";
  var codChargeGrossValueExpr = "0";
  var giftChargeNetValueExpr = "0";
  var giftChargeVatValueExpr = "0";
  var giftChargeGrossValueExpr = "0";
  var otherChargeNetValueExpr = "0";
  var otherChargeVatValueExpr = "0";
  var otherChargeGrossValueExpr = "0";
  var expenseApplySql = "";
  var docNetTotal = _bv_col_expr("F", "FINDOC", ["NETAMNT"], c.sumAmount);
  var docGrossTotal = _bv_col_expr("F", "FINDOC", ["SUMAMNT"], "(ISNULL(" + docNetTotal + ",0) + ISNULL(" + c.taxAmount + ",0))");
  var salesSign =
    "(CASE " +
    "WHEN ISNULL(F.SOSOURCE,0)=1351 AND ISNULL(F.TFPRMS,0) IN (151,152,181) THEN -1 " +
    "WHEN ISNULL(F.SOSOURCE,0)<>1351 AND ISNULL(F.TFPRMS,0) IN (102,181) THEN -1 " +
    "ELSE 1 END)";
  var findocExpensesNet = _bv_col_expr("F", "FINDOC", ["EXPN", "TEXPN", "LEXPN"], "0");
  var saldocExpensesRaw = _bv_col_expr("SD", "SALDOC", ["EXPN"], findocExpensesNet);
  var saldocExpensesNet = "(" + salesSign + " * COALESCE(TRY_CAST(ISNULL(" + saldocExpensesRaw + ",0) AS decimal(28,8)),0))";
  var paymentExpr =
    "COALESCE(NULLIF(" +
    originPaymentName +
    ",''), NULLIF(" +
    originPaymentCode +
    ",''), NULLIF(CAST(ISNULL(" +
    originPayment +
    ",0) AS VARCHAR(128)),'0'), NULLIF(" +
    paymentName +
    ",''), NULLIF(" +
    paymentCode +
    ",''), NULLIF(CAST(ISNULL(" +
    docPayment +
    ",0) AS VARCHAR(128)),'0'), '')";
  var documentStatusExpr =
    "(CASE " +
    "WHEN ISNULL(" + isCancel + ",0)=1 THEN N'Cancelled' " +
    "WHEN ISNULL(" + finStates + ",0) IN (10) THEN N'Completed' " +
    "WHEN ISNULL(" + finStates + ",0) IN (3,4,6) THEN N'Closed' " +
    "WHEN ISNULL(" + finStates + ",0) IN (1,2) THEN N'Open' " +
    "ELSE N'Open' END)";
  var lQty = _bv_col_expr("L", "MTRLINES", ["QTY1", "QTY"], "0");
  var lNet = _bv_col_expr("L", "MTRLINES", ["NETLINEVAL", "NETVAL", "NETAMNT", "LINEVAL"], "0");
  var lLineValue = _bv_col_expr("L", "MTRLINES", ["LINEVAL", "NETLINEVAL", "NETVAL", "NETAMNT"], "0");
  var lVat = _bv_col_expr("L", "MTRLINES", ["VATAMNT", "TAXAMNT", "FPAAMNT", "LINEVAT", "LINETAX", "LINEVATAMNT", "LINETAXAMNT"], "NULL");
  var lGross = _bv_col_expr("L", "MTRLINES", ["GROSSVAL", "SUMAMNT", "TOTVAL", "LINEGROSS"], "NULL");
  // Cost of goods sold = the item's AVERAGE ACQUISITION COST (Μέση Τιμή Κτήσης), which
  // SoftOne maintains per item in FMATTOTALS.PURMTK (wholesale net of all purchase
  // discounts / payment terms). NOT the wholesale list price (PRICEW) — that is only the
  // pre-discount starting price and is NOT the acquisition cost.
  // Priority (mirrors the SQL querypack sales cost exactly — parity):
  //   1) a real line cost if SoftOne stored one (SALESCVAL — itself the PURMTK-based COGS
  //      booked at sale time), else
  //   2) FMATTOTALS.PURMTK x qty (the per-item weighted-average acquisition cost), else
  //   3) a *plausible* MTRBALSHEET moving-average (<= unit sell price; corrupt rows out), else
  //   4) GROSS (see lCost after grossExpr) so an unknown-cost line yields ~0 profit.
  var lRealCost = _bv_col_expr("L", "MTRLINES", ["SALESCVAL", "COSTVAL", "COSTVALUE", "LCOST"], "NULL");
  var fmtTable = _bv_table_exists("FMATTOTALS5") ? "FMATTOTALS5" : (_bv_table_exists("FMATTOTALS") ? "FMATTOTALS" : "");
  var bvHasFmt = fmtTable !== "";
  var fmtPurmtk = bvHasFmt ? "ISNULL(FMT.PURMTK,0)" : "0";
  var bvHasMac = _bv_table_exists("MTRBALSHEET");
  var macUnitCost = bvHasMac ? "ISNULL(MAC.UNIT_COST,0)" : "0";
  var absLineQty = "ABS(ISNULL(" + lQty + ",0))";
  var absLineNet = "ABS(ISNULL(" + lNet + ",0))";
  var unitNetPrice = "(CASE WHEN " + absLineQty + " > 0 THEN " + absLineNet + " / " + absLineQty + " ELSE " + absLineNet + " END)";
  var cogsUnit = "COALESCE(NULLIF(" + fmtPurmtk + ",0), CASE WHEN " + macUnitCost + " > 0 AND " + macUnitCost + " <= " + unitNetPrice + " THEN " + macUnitCost + " ELSE NULL END)";
  var docNetAbsTotal = "NULLIF(SUM(ABS(ISNULL(" + lNet + ",0))) OVER (PARTITION BY " + c.findoc + "),0)";
  var lineTaxAbs = "ABS(ISNULL(" + lVat + ",0))";
  var lineTaxDocAbsTotal = "SUM(" + lineTaxAbs + ") OVER (PARTITION BY " + c.findoc + ")";
  var headerTaxAbs = "ABS(ISNULL(" + c.taxAmount + ",0))";
  var vatExpr =
    "(CASE WHEN " +
    lineTaxDocAbsTotal +
    " > 0 THEN " +
    lineTaxAbs +
    " WHEN " +
    docNetAbsTotal +
    " IS NOT NULL THEN (" +
    headerTaxAbs +
    " * ABS(ISNULL(" +
    lNet +
    ",0)) / " +
    docNetAbsTotal +
    ") ELSE 0 END)";
  var grossExpr = "ABS(ISNULL(" + lNet + ",0)) + " + vatExpr;
  // Unsigned line COGS (output applies the sales sign). Priority:
  //   1) qty x PURMTK / plausible moving-average (cogsUnit) — the authoritative avg
  //      acquisition cost; then
  //   2) a real line cost ONLY when it is plausible (0 < cost <= net). Older documents in
  //      some SoftOne installs store the GROSS line value in SALESCVAL/COSTVAL, so a line
  //      cost above the net selling price is rejected as unreliable; then
  //   3) NET so an unknown-cost line yields profit = net - net = 0 ("unknown margin").
  // NET (not gross) because BI computes gross profit as net - cost.
  var plausibleLineCost = "CASE WHEN ABS(" + lRealCost + ") > 0 AND ABS(" + lRealCost + ") <= " + absLineNet + " THEN ABS(" + lRealCost + ") ELSE NULL END";
  var lCost = "COALESCE(NULLIF(" + absLineQty + " * (" + cogsUnit + "),0), " + plausibleLineCost + ", (" + absLineNet + "))";

  if (_bv_table_exists("EXPANAL")) {
    var expFindoc = _bv_col_expr("EA", "EXPANAL", ["FINDOC"], c.findoc);
    var expCompany = _bv_col_expr("EA", "EXPANAL", ["COMPANY"], c.company);
    var expValue = _bv_col_expr("EA", "EXPANAL", ["EXPVAL", "TEXPVAL", "LEXPVAL", "EXPVAL0"], "0");
    var expVat = _bv_col_expr("EA", "EXPANAL", ["EXPVATVAL", "TEXPVATVAL", "LEXPVATVAL", "VATAMNT", "TAXAMNT", "FPAAMNT", "VATVAL", "FPAVAL", "VAT_AMOUNT", "TAX_AMOUNT", "FPA_AMOUNT", "VAT", "FPA"], "0");
    var expDesc = _bv_col_expr("EA", "EXPANAL", ["EXPNAME", "NAME", "DESCR", "DESCRIPTION", "TITLE", "EXPENSE", "EXPENSES"], "''");
    var expDesc2 = _bv_col_expr("EA2", "EXPANAL", ["EXPNAME", "NAME", "DESCR", "DESCRIPTION", "TITLE", "EXPENSE", "EXPENSES"], "''");
    var expCode = _bv_col_expr("EA", "EXPANAL", ["EXPN"], "NULL");
    var expCode2 = _bv_col_expr("EA2", "EXPANAL", ["EXPN"], "NULL");
    var expName = _bv_table_exists("EXPN") ? ("COALESCE(" + _bv_col_expr("EN", "EXPN", ["NAME"], "NULL") + "," + expDesc + ")") : expDesc;
    var expName2 = _bv_table_exists("EXPN") ? ("COALESCE(" + _bv_col_expr("EN2", "EXPN", ["NAME"], "NULL") + "," + expDesc2 + ")") : expDesc2;
    var expJoin = _bv_table_exists("EXPN") ? (" LEFT JOIN EXPN EN ON EN.EXPN=" + expCode + " AND EN.COMPANY=EA.COMPANY ") : "";
    var expJoin2 = _bv_table_exists("EXPN") ? (" LEFT JOIN EXPN EN2 ON EN2.EXPN=" + expCode2 + " AND EN2.COMPANY=EA2.COMPANY ") : "";
    shippingExpenseValueExpr = "ISNULL(EXA.CHARGE_REVENUE_NET_VALUE,0)";
    shippingExpenseDescriptionExpr = "ISNULL(EXA.SHIPPING_EXPENSE_DESCRIPTION,'')";
    chargeRevenueNetValueExpr = "ISNULL(EXA.CHARGE_REVENUE_NET_VALUE,0)";
    chargeRevenueVatValueExpr = "ISNULL(EXA.CHARGE_REVENUE_VAT_VALUE,0)";
    chargeRevenueGrossValueExpr = "ISNULL(EXA.CHARGE_REVENUE_GROSS_VALUE,0)";
    chargeRevenueDescriptionExpr = "ISNULL(EXA.CHARGE_REVENUE_DESCRIPTION,'')";
    chargeRevenueLinesJsonExpr = "ISNULL(EXA.CHARGE_REVENUE_LINES_JSON,'[]')";
    shippingChargeNetValueExpr = "ISNULL(EXA.SHIPPING_CHARGE_NET_VALUE,0)";
    shippingChargeVatValueExpr = "ISNULL(EXA.SHIPPING_CHARGE_VAT_VALUE,0)";
    shippingChargeGrossValueExpr = "ISNULL(EXA.SHIPPING_CHARGE_GROSS_VALUE,0)";
    codChargeNetValueExpr = "ISNULL(EXA.COD_CHARGE_NET_VALUE,0)";
    codChargeVatValueExpr = "ISNULL(EXA.COD_CHARGE_VAT_VALUE,0)";
    codChargeGrossValueExpr = "ISNULL(EXA.COD_CHARGE_GROSS_VALUE,0)";
    giftChargeNetValueExpr = "ISNULL(EXA.GIFT_CHARGE_NET_VALUE,0)";
    giftChargeVatValueExpr = "ISNULL(EXA.GIFT_CHARGE_VAT_VALUE,0)";
    giftChargeGrossValueExpr = "ISNULL(EXA.GIFT_CHARGE_GROSS_VALUE,0)";
    otherChargeNetValueExpr = "ISNULL(EXA.OTHER_CHARGE_NET_VALUE,0)";
    otherChargeVatValueExpr = "ISNULL(EXA.OTHER_CHARGE_VAT_VALUE,0)";
    otherChargeGrossValueExpr = "ISNULL(EXA.OTHER_CHARGE_GROSS_VALUE,0)";
    expenseApplySql =
      " OUTER APPLY (" +
      "SELECT " +
      "CAST(CASE WHEN ABS(COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0)) > 0 THEN COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0) ELSE COALESCE(SUM(EX.exp_net_value),0) END AS FLOAT) AS SHIPPING_EXPENSE_VALUE," +
      "CAST(ISNULL(STUFF((SELECT DISTINCT ' | ' + CAST(NULLIF(LTRIM(RTRIM(" + expName2 + ")), '') AS NVARCHAR(255)) " +
      "FROM EXPANAL EA2 " +
      expJoin2 +
      "WHERE " + _bv_col_expr("EA2", "EXPANAL", ["FINDOC"], c.findoc) + "=" + c.findoc + " AND " + _bv_col_expr("EA2", "EXPANAL", ["COMPANY"], c.company) + "=F.COMPANY " +
      "AND NULLIF(LTRIM(RTRIM(" + expName2 + ")), '') IS NOT NULL " +
      "FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'),1,3,''),'') AS NVARCHAR(1024)) AS SHIPPING_EXPENSE_DESCRIPTION," +
      "CAST(CASE WHEN ABS(COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0)) > 0 THEN COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0) ELSE COALESCE(SUM(EX.exp_net_value),0) END AS FLOAT) AS CHARGE_REVENUE_NET_VALUE," +
      "CAST(COALESCE(SUM(EX.exp_vat_value),0) AS FLOAT) AS CHARGE_REVENUE_VAT_VALUE," +
      "CAST((CASE WHEN ABS(COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0)) > 0 THEN COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0) ELSE COALESCE(SUM(EX.exp_net_value),0) END) + COALESCE(SUM(EX.exp_vat_value),0) AS FLOAT) AS CHARGE_REVENUE_GROSS_VALUE," +
      "CAST(ISNULL(STUFF((SELECT DISTINCT ' | ' + CAST(NULLIF(LTRIM(RTRIM(" + expName2 + ")), '') AS NVARCHAR(255)) " +
      "FROM EXPANAL EA2 " +
      expJoin2 +
      "WHERE " + _bv_col_expr("EA2", "EXPANAL", ["FINDOC"], c.findoc) + "=" + c.findoc + " AND " + _bv_col_expr("EA2", "EXPANAL", ["COMPANY"], c.company) + "=F.COMPANY " +
      "AND NULLIF(LTRIM(RTRIM(" + expName2 + ")), '') IS NOT NULL " +
      "FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'),1,3,''),'') AS NVARCHAR(1024)) AS CHARGE_REVENUE_DESCRIPTION," +
      "CAST(ISNULL((SELECT EA2.* FROM EXPANAL EA2 WHERE " + _bv_col_expr("EA2", "EXPANAL", ["FINDOC"], c.findoc) + "=" + c.findoc + " AND " + _bv_col_expr("EA2", "EXPANAL", ["COMPANY"], c.company) + "=F.COMPANY FOR JSON PATH),'[]') AS NVARCHAR(MAX)) AS CHARGE_REVENUE_LINES_JSON," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='shipping' THEN EX.exp_net_value ELSE 0 END),0) AS FLOAT) AS SHIPPING_CHARGE_NET_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='shipping' THEN EX.exp_vat_value ELSE 0 END),0) AS FLOAT) AS SHIPPING_CHARGE_VAT_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='shipping' THEN EX.exp_gross_value ELSE 0 END),0) AS FLOAT) AS SHIPPING_CHARGE_GROSS_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='cod' THEN EX.exp_net_value ELSE 0 END),0) AS FLOAT) AS COD_CHARGE_NET_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='cod' THEN EX.exp_vat_value ELSE 0 END),0) AS FLOAT) AS COD_CHARGE_VAT_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='cod' THEN EX.exp_gross_value ELSE 0 END),0) AS FLOAT) AS COD_CHARGE_GROSS_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='gift' THEN EX.exp_net_value ELSE 0 END),0) AS FLOAT) AS GIFT_CHARGE_NET_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='gift' THEN EX.exp_vat_value ELSE 0 END),0) AS FLOAT) AS GIFT_CHARGE_VAT_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='gift' THEN EX.exp_gross_value ELSE 0 END),0) AS FLOAT) AS GIFT_CHARGE_GROSS_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='other' THEN EX.exp_net_value ELSE 0 END),0) + CASE WHEN ABS(COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0)) > 0 AND ABS(COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0) - COALESCE(SUM(EX.exp_net_value),0)) > 0.004 THEN COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0) - COALESCE(SUM(EX.exp_net_value),0) ELSE 0 END AS FLOAT) AS OTHER_CHARGE_NET_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='other' THEN EX.exp_vat_value ELSE 0 END),0) AS FLOAT) AS OTHER_CHARGE_VAT_VALUE," +
      "CAST(COALESCE(SUM(CASE WHEN EX.expense_group='other' THEN EX.exp_gross_value ELSE 0 END),0) + CASE WHEN ABS(COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0)) > 0 AND ABS(COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0) - COALESCE(SUM(EX.exp_net_value),0)) > 0.004 THEN COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0) - COALESCE(SUM(EX.exp_net_value),0) ELSE 0 END AS FLOAT) AS OTHER_CHARGE_GROSS_VALUE " +
      "FROM (" +
      "SELECT " +
      "LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) AS exp_name_norm," +
      "(" + salesSign + " * COALESCE(TRY_CAST(ISNULL(" + expValue + ",0) AS decimal(28,8)),0)) AS exp_net_value," +
      "(" + salesSign + " * COALESCE(TRY_CAST(ISNULL(" + expVat + ",0) AS decimal(28,8)),0)) AS exp_vat_value," +
      "(" + salesSign + " * (COALESCE(TRY_CAST(ISNULL(" + expValue + ",0) AS decimal(28,8)),0) + COALESCE(TRY_CAST(ISNULL(" + expVat + ",0) AS decimal(28,8)),0))) AS exp_gross_value," +
      "(CASE " +
      "WHEN LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE N'%αντικαταβολ%' OR LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE '%cod%' THEN 'cod' " +
      "WHEN LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE N'%δωρ%' OR LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE '%gift%' THEN 'gift' " +
      "WHEN LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE N'%αποστολ%' OR LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE N'%μεταφορ%' OR LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE '%courier%' OR LOWER(ISNULL(NULLIF(LTRIM(RTRIM(" + expName + ")),''),'')) LIKE '%ship%' THEN 'shipping' " +
      "ELSE 'other' END) AS expense_group " +
      "FROM EXPANAL EA " + expJoin + "WHERE " + expFindoc + "=" + c.findoc + " AND " + expCompany + "=F.COMPANY" +
      ") EX" +
      ") EXA ";
  } else {
    shippingExpenseValueExpr = "COALESCE(TRY_CAST(ISNULL(" + saldocExpensesNet + ",0) AS decimal(28,8)),0)";
    chargeRevenueNetValueExpr = shippingExpenseValueExpr;
    chargeRevenueGrossValueExpr = shippingExpenseValueExpr;
    otherChargeNetValueExpr = shippingExpenseValueExpr;
    otherChargeGrossValueExpr = shippingExpenseValueExpr;
  }

  var mdWhouse = _bv_col_expr("MD", "MTRDOC", ["WHOUSE"], "0");
  var whName = _bv_col_expr("W", "WHOUSE", ["NAME"], "''");

  var whereSql =
    " WHERE F.COMPANY=" +
    cfg.company +
    " AND ISNULL(" +
    c.sodtype +
    ",0)=13 AND ((" +
    "ISNULL(" + c.sosource + ",0)=1351 AND ISNULL(" + c.soredir + ",0) IN (0,10000) AND ISNULL(F.TFPRMS,0) IN (102,103,131,151,152,181)" +
    ") OR (" +
    "ISNULL(" + c.sosource + ",0)<>1351 AND ISNULL(F.TFPRMS,0) IN (101,102,131,181)" +
    "))";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) + '-' + CAST(ISNULL(" +
    lLineId +
    ",0) AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" +
    c.finCode +
    " AS VARCHAR(128)) AS DOCUMENT_NO," +
    "CAST(" +
    c.series +
    " AS VARCHAR(128)) AS DOCUMENT_SERIES," +
    seriesInfo.seriesNameExpr +
    " AS DOCUMENT_SERIES_NAME," +
    "'sales_' + CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS VARCHAR(16)) AS DOCUMENT_TYPE," +
    "CAST(" +
    documentStatusExpr +
    " AS VARCHAR(128)) AS DOCUMENT_STATUS," +
    "CONVERT(VARCHAR(10), " +
    c.trnDate +
    ", 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" +
    c.updDate +
    ", " +
    c.trnDate +
    "), 126) AS UPDATED_AT," +
    "CONVERT(VARCHAR(19), " +
    sourceCreatedAt +
    ", 126) AS SOURCE_CREATED_AT," +
    "CAST(ISNULL(" +
    c.branch +
    ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr +
    " AS BRANCH_NAME," +
    "CAST(ISNULL(" +
    c.company +
    ",0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CAST(ISNULL(" +
    mdWhouse +
    ",0) AS VARCHAR(64)) AS WAREHOUSE_EXT_ID," +
    "CAST(ISNULL(" +
    whName +
    ", CAST(ISNULL(" +
    mdWhouse +
    ",0) AS VARCHAR(255))) AS VARCHAR(255)) AS WAREHOUSE_NAME," +
    "CAST(ISNULL(C.CODE, " +
    c.trdr +
    ") AS VARCHAR(128)) AS CUSTOMER_EXT_ID," +
    "CAST(ISNULL(C.NAME, '') AS VARCHAR(255)) AS CUSTOMER_NAME," +
    "CAST(ISNULL(" +
    _bv_col_expr("C", "TRDR", ["AFM"], "''") +
    ", '') AS VARCHAR(64)) AS CUSTOMER_AFM," +
    "CAST(ISNULL(I.CODE, " +
    lMtrl +
    ") AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" +
    iName +
    ", '') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(NULLIF(CAST(ISNULL(" +
    itemBrand +
    ",0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS BRAND_EXTERNAL_ID," +
    "CAST(ISNULL(" +
    brandName +
    ", '') AS VARCHAR(255)) AS BRAND_NAME," +
    "CAST(NULLIF(CAST(ISNULL(" +
    itemGroup +
    ",0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS GROUP_EXT_ID," +
    "CAST(ISNULL(" +
    groupName +
    ", '') AS VARCHAR(255)) AS GROUP_NAME," +
    "CAST(NULL AS VARCHAR(64)) AS SUPPLIER_EXTERNAL_ID," +
    "CAST(NULL AS VARCHAR(64)) AS CATEGORY_EXTERNAL_ID," +
    "CAST(ISNULL(" +
    manualOrderCategoryExpr +
    ", '') AS VARCHAR(128)) AS MANUAL_ORDER_CATEGORY," +
    "CAST(ISNULL(" +
    commercialStatusExpr +
    ", '') AS VARCHAR(128)) AS COMMERCIAL_STATUS," +
    "CAST(0 AS FLOAT) AS DISCOUNT_PCT," +
    "CAST(0 AS FLOAT) AS DISCOUNT_AMOUNT," +
    "CAST(" +
    docChannelResolved +
    " AS VARCHAR(64)) AS CHANNEL_EXT_ID," +
    "CAST(COALESCE(NULLIF(" +
    channelName +
    ", ''), CASE WHEN NULLIF(" +
    eshopCode +
    ", '') IS NOT NULL THEN 'Site' ELSE '' END) AS VARCHAR(255)) AS CHANNEL_NAME," +
    "CAST(ISNULL(" +
    eshopCode +
    ", '') AS VARCHAR(128)) AS ESHOP_CODE," +
    "CAST(" +
    paymentExpr +
    " AS VARCHAR(128)) AS PAYMENT_METHOD," +
    "CAST(ISNULL(" +
    shippingMethod +
    ", '') AS VARCHAR(128)) AS SHIPPING_METHOD," +
    "CAST(ISNULL(" +
    reasonExpr +
    ", '') AS VARCHAR(255)) AS REASON," +
    "CAST(COALESCE(NULLIF(" +
    originCustomerBranch +
    ", ''), NULLIF(" +
    customerBranch +
    ", ''), '') AS VARCHAR(255)) AS CUSTOMER_BRANCH," +
    "CAST(ISNULL(ORIG.FINCODE, '') AS VARCHAR(128)) AS ORIGIN_REF," +
    "CAST(ISNULL(F.FINCODE, '') AS VARCHAR(128)) AS DESTINATION_REF," +
    "CAST(ISNULL(" +
    deliveryAddress +
    ", '') AS VARCHAR(255)) AS DELIVERY_ADDRESS," +
    "CAST(ISNULL(" +
    deliveryZip +
    ", '') AS VARCHAR(64)) AS DELIVERY_ZIP," +
    "CAST(ISNULL(" +
    deliveryCity +
    ", '') AS VARCHAR(255)) AS DELIVERY_CITY," +
    "CAST(ISNULL(" +
    deliveryArea +
    ", '') AS VARCHAR(255)) AS DELIVERY_AREA," +
    "CAST(COALESCE(NULLIF(" +
    movementTypeName +
    ", ''), CASE WHEN ISNULL(" +
    lQty +
    ",0) >= 0 THEN 'entry' ELSE 'exit' END) AS VARCHAR(128)) AS MOVEMENT_TYPE," +
    "CAST(ISNULL(" +
    carrierName +
    ", '') AS VARCHAR(255)) AS CARRIER_NAME," +
    "CAST(ISNULL(" +
    transportMedium +
    ", '') AS VARCHAR(255)) AS TRANSPORT_MEDIUM," +
    "CAST(ISNULL(" +
    transportNo +
    ", '') AS VARCHAR(128)) AS TRANSPORT_NO," +
    "CAST(ISNULL(" +
    routeName +
    ", '') AS VARCHAR(255)) AS ROUTE_NAME," +
    "CONVERT(VARCHAR(10), " +
    loadingDate +
    ", 23) AS LOADING_DATE," +
    "CONVERT(VARCHAR(10), " +
    deliveryDate +
    ", 23) AS DELIVERY_DATE," +
    "CAST(" +
    voucherNo +
    " AS VARCHAR(128)) AS VOUCHER_NO," +
    "CAST(" +
    voucherUrl +
    " AS VARCHAR(1024)) AS VOUCHER_URL," +
    "CAST(" +
    lockerId +
    " AS VARCHAR(128)) AS LOCKER_ID," +
    "CAST(" +
    orderComments +
    " AS VARCHAR(1024)) AS ORDER_COMMENTS," +
    "CAST(" +
    shippingComments +
    " AS VARCHAR(1024)) AS SHIPPING_COMMENTS," +
    "CAST(" +
    giftComments +
    " AS VARCHAR(1024)) AS GIFT_COMMENTS," +
    "CAST(" +
    carrierName +
    " AS VARCHAR(255)) AS MARKETPLACE_COURIER," +
    "CAST(" +
    marketplaceInternalId +
    " AS VARCHAR(128)) AS MARKETPLACE_INTERNAL_ID," +
    "CAST(" +
    shippingExpenseValueExpr +
    " AS FLOAT) AS SHIPPING_EXPENSE_VALUE," +
    "CAST(" +
    shippingExpenseDescriptionExpr +
    " AS VARCHAR(1024)) AS SHIPPING_EXPENSE_DESCRIPTION," +
    "CAST(" +
    chargeRevenueNetValueExpr +
    " AS FLOAT) AS CHARGE_REVENUE_NET_VALUE," +
    "CAST(" +
    chargeRevenueVatValueExpr +
    " AS FLOAT) AS CHARGE_REVENUE_VAT_VALUE," +
    "CAST(" +
    chargeRevenueGrossValueExpr +
    " AS FLOAT) AS CHARGE_REVENUE_GROSS_VALUE," +
    "CAST(" +
    chargeRevenueDescriptionExpr +
    " AS VARCHAR(1024)) AS CHARGE_REVENUE_DESCRIPTION," +
    "CAST(" +
    chargeRevenueLinesJsonExpr +
    " AS VARCHAR(MAX)) AS CHARGE_REVENUE_LINES_JSON," +
    "CAST(" +
    shippingChargeNetValueExpr +
    " AS FLOAT) AS SHIPPING_CHARGE_NET_VALUE," +
    "CAST(" +
    shippingChargeVatValueExpr +
    " AS FLOAT) AS SHIPPING_CHARGE_VAT_VALUE," +
    "CAST(" +
    shippingChargeGrossValueExpr +
    " AS FLOAT) AS SHIPPING_CHARGE_GROSS_VALUE," +
    "CAST(" +
    codChargeNetValueExpr +
    " AS FLOAT) AS COD_CHARGE_NET_VALUE," +
    "CAST(" +
    codChargeVatValueExpr +
    " AS FLOAT) AS COD_CHARGE_VAT_VALUE," +
    "CAST(" +
    codChargeGrossValueExpr +
    " AS FLOAT) AS COD_CHARGE_GROSS_VALUE," +
    "CAST(" +
    giftChargeNetValueExpr +
    " AS FLOAT) AS GIFT_CHARGE_NET_VALUE," +
    "CAST(" +
    giftChargeVatValueExpr +
    " AS FLOAT) AS GIFT_CHARGE_VAT_VALUE," +
    "CAST(" +
    giftChargeGrossValueExpr +
    " AS FLOAT) AS GIFT_CHARGE_GROSS_VALUE," +
    "CAST(" +
    otherChargeNetValueExpr +
    " AS FLOAT) AS OTHER_CHARGE_NET_VALUE," +
    "CAST(" +
    otherChargeVatValueExpr +
    " AS FLOAT) AS OTHER_CHARGE_VAT_VALUE," +
    "CAST(" +
    otherChargeGrossValueExpr +
    " AS FLOAT) AS OTHER_CHARGE_GROSS_VALUE," +
    "CAST(" +
    salesSign +
    " * ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS QTY," +
    "CAST(" +
    salesSign +
    " * ISNULL(" +
    lNet +
    ",0) AS FLOAT) AS NET_VALUE," +
    "CAST(" +
    salesSign +
    " * (" +
    grossExpr +
    ")" +
    " AS FLOAT) AS GROSS_VALUE," +
    "CAST(" +
    salesSign +
    " * (" +
    vatExpr +
    ")" +
    " AS FLOAT) AS VAT_AMOUNT," +
    "CAST(" +
    salesSign +
    " * ISNULL(" +
    docNetTotal +
    ",0) AS FLOAT) AS DOC_NET_TOTAL," +
    "CAST(" +
    salesSign +
    " * ISNULL(" +
    saldocExpensesRaw +
    ",0) AS FLOAT) AS DOC_EXPENSES_TOTAL," +
    "CAST(" +
    salesSign +
    " * ISNULL(" +
    c.taxAmount +
    ",0) AS FLOAT) AS DOC_TAX_TOTAL," +
    "CAST(" +
    salesSign +
    " * ISNULL(" +
    docGrossTotal +
    ",0) AS FLOAT) AS DOC_GROSS_TOTAL," +
    "CAST(" +
    salesSign +
    " * ISNULL(" +
    lCost +
    ",0) AS FLOAT) AS COST_AMOUNT," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(ISNULL(" +
    c.soredir +
    ",0) AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(ISNULL(" +
    c.sodtype +
    ",0) AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(F.TFPRMS,0) AS INT) AS SOURCE_TRANSACTION_TYPE_ID," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS INT) AS OBJECT_ID " +
    "FROM FINDOC F " +
    "INNER JOIN MTRLINES L ON L.FINDOC=" +
    c.findoc +
    " AND L.COMPANY=F.COMPANY " +
    "OUTER APPLY (SELECT TOP 1 ORIGF.* FROM FINDOC ORIGF WHERE ORIGF.FINDOC=NULLIF(" +
    lFindocs +
    ",0) AND ORIGF.COMPANY=L.COMPANY) ORIG " +
    "LEFT JOIN MTRDOC MD ON MD.FINDOC=" +
    c.findoc +
    " AND MD.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRDOC OMD ON OMD.FINDOC=ORIG.FINDOC AND OMD.COMPANY=F.COMPANY " +
    "LEFT JOIN WHOUSE W ON W.WHOUSE=" +
    mdWhouse +
    " AND W.COMPANY=F.COMPANY " +
    "LEFT JOIN TRDR C ON C.TRDR=" +
    c.trdr +
    " AND C.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRL I ON I.MTRL=" +
    lMtrl +
    " AND I.COMPANY=F.COMPANY " +
    itemExtraJoinSql +
    // Per-item average acquisition cost (Μέση Τιμή Κτήσης) that SoftOne maintains in
    // FMATTOTALS.PURMTK — the primary COGS source used in lCost above.
    (bvHasFmt ? ("LEFT JOIN (SELECT COMPANY, MTRL, MAX(NULLIF(ISNULL(PURMTK,0),0)) AS PURMTK FROM " + fmtTable + " WHERE COMPANY=" + cfg.company + " GROUP BY COMPANY, MTRL) FMT ON FMT.COMPANY=L.COMPANY AND FMT.MTRL=L.MTRL ") : "") +
    // Moving-average unit cost per item (weighted purchase cost from MTRBALSHEET),
    // used as COGS fallback in lCost above when PURMTK is unavailable.
    (bvHasMac ? ("LEFT JOIN (SELECT COMPANY, MTRL, CASE WHEN SUM(ISNULL(PURQTY,0))>0 THEN SUM(ISNULL(PURVAL,0))/SUM(ISNULL(PURQTY,0)) ELSE 0 END AS UNIT_COST FROM MTRBALSHEET WHERE COMPANY=" + cfg.company + " GROUP BY COMPANY, MTRL) MAC ON MAC.COMPANY=L.COMPANY AND MAC.MTRL=L.MTRL ") : "") +
    "LEFT JOIN MTRMARK MK ON MK.MTRMARK=I.MTRMARK AND MK.COMPANY=I.COMPANY " +
    "LEFT JOIN MTRGROUP MG ON MG.MTRGROUP=I.MTRGROUP AND MG.COMPANY=I.COMPANY " +
    (_bv_table_exists("SALDOC") ? ("LEFT JOIN SALDOC SD ON SD.FINDOC=" + c.findoc + " AND SD.COMPANY=F.COMPANY ") : "") +
    "LEFT JOIN TRDBRANCH CB ON CB.TRDBRANCH=" +
    docCustomerBranchId +
    " AND (CB.COMPANY=F.COMPANY OR CB.COMPANY=0) " +
    "LEFT JOIN TRDBRANCH OCB ON OCB.TRDBRANCH=" +
    originCustomerBranchId +
    " AND (OCB.COMPANY=F.COMPANY OR OCB.COMPANY=0) " +
    "LEFT JOIN TRDR CA ON CA.TRDR=ISNULL(" +
    docCarrierTrdr +
    ",0) AND (CA.COMPANY=F.COMPANY OR CA.COMPANY=0) " +
    expenseApplySql +
    "OUTER APPLY (SELECT TOP 1 E.CCC88ECHANNEL, E.NAME FROM CCC88ECHANNEL E WHERE E.CCC88ECHANNEL=" +
    docChannelResolved +
    " AND (E.COMPANY=F.COMPANY OR E.COMPANY=1001) ORDER BY CASE WHEN E.COMPANY=F.COMPANY THEN 0 ELSE 1 END) EC " +
    "OUTER APPLY (SELECT TOP 1 OP.CODE, OP.NAME FROM PAYMENT OP WHERE OP.PAYMENT=" +
    originPayment +
    " AND OP.SODTYPE=" +
    originSodtype +
    " AND (OP.COMPANY=F.COMPANY OR OP.COMPANY=1000) ORDER BY CASE WHEN OP.COMPANY=F.COMPANY THEN 0 ELSE 1 END) OPM " +
    "OUTER APPLY (SELECT TOP 1 P.CODE, P.NAME FROM PAYMENT P WHERE P.PAYMENT=" +
    docPayment +
    " AND P.SODTYPE=" +
    c.sodtype +
    " AND (P.COMPANY=F.COMPANY OR P.COMPANY=1000) ORDER BY CASE WHEN P.COMPANY=F.COMPANY THEN 0 ELSE 1 END) PM " +
    branchInfo.joinSql +
    seriesInfo.joinSql +
    whereSql +
    " ORDER BY " +
    c.trnDate +
    " ASC, " +
    c.findoc +
    " ASC, " +
    lLineId +
    " ASC";
  return sql;
}

function _bv_purchases_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var seriesInfo = _bv_series_info_expr(c);
  var branchInfo = _bv_branch_info_expr(c);

  var lLineId = _bv_col_expr("L", "MTRLINES", ["MTRLINES", "LINENUM"], "0");
  var lMtrl = _bv_col_expr("L", "MTRLINES", ["MTRL"], "0");
  var iName = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var lQty = _bv_col_expr("L", "MTRLINES", ["QTY1", "QTY"], "0");
  var lNet = _bv_col_expr("L", "MTRLINES", ["NETLINEVAL", "NETVAL", "NETAMNT", "LINEVAL"], "0");
  var lLineValue = _bv_col_expr("L", "MTRLINES", ["LINEVAL", "NETLINEVAL", "NETVAL", "NETAMNT"], "0");
  var lVat = _bv_col_expr("L", "MTRLINES", ["VATAMNT", "TAXAMNT", "FPAAMNT", "LINEVAT", "LINETAX", "LINEVATAMNT", "LINETAXAMNT"], "NULL");
  var lGross = _bv_col_expr("L", "MTRLINES", ["GROSSVAL", "SUMAMNT", "TOTVAL", "LINEGROSS"], "NULL");
  var lCost = _bv_col_expr("L", "MTRLINES", ["COSTVAL", "COSTVALUE", "LCOST", "COST", "LINEVAL"], lNet);
  var lPrice = _bv_col_expr("L", "MTRLINES", ["PRICE", "PRICEM", "PRICELISTVALUE"], "NULL");
  var lNoDiscountFallback = "(ABS(ISNULL(" + lPrice + ",0)) * ABS(ISNULL(" + lQty + ",0)))";
  var lNoDiscount = _bv_col_expr("L", "MTRLINES", ["NODSCAMNT"], lNoDiscountFallback);
  var disc1Pct = _bv_col_expr("L", "MTRLINES", ["DISC1PRC", "DISC1", "DISCOUNT1"], "NULL");
  var disc2Pct = _bv_col_expr("L", "MTRLINES", ["DISC2PRC", "DISC2", "DISCOUNT2"], "NULL");
  var disc3Pct = _bv_col_expr("L", "MTRLINES", ["DISC3PRC", "DISC3", "DISCOUNT3"], "NULL");
  var disc1Val = _bv_col_expr("L", "MTRLINES", ["DISC1VAL", "DISCOUNT1VAL", "DISCVAL1"], "0");
  var disc2Val = _bv_col_expr("L", "MTRLINES", ["DISC2VAL", "DISCOUNT2VAL", "DISCVAL2"], "0");
  var disc3Val = _bv_col_expr("L", "MTRLINES", ["DISC3VAL", "DISCOUNT3VAL", "DISCVAL3"], "0");
  var paymentCode = _bv_col_expr("PM", "PAYMENT", ["CODE"], "''");
  var paymentName = _bv_col_expr("PM", "PAYMENT", ["NAME"], "''");
  var docPayment = _bv_col_expr("F", "FINDOC", ["PAYMENT"], "0");
  var sourceCreatedAt = "COALESCE(" + _bv_col_expr("F", "FINDOC", ["SOTIME", "INSDATE", "CREATED", "CREATEDAT", "INSERTDATE"], "NULL") + ", " + c.updDate + ")";
  var paymentExpr =
    "COALESCE(NULLIF(" +
    paymentName +
    ",''), NULLIF(" +
    paymentCode +
    ",''), NULLIF(CAST(ISNULL(" +
    docPayment +
    ",0) AS VARCHAR(128)),'0'), '')";
  var manualOrderInfo = _bv_item_manual_order_info("I");
  var manualOrderCategoryExpr = manualOrderInfo.expr;
  var commercialStatusExpr = manualOrderInfo.commercialStatusExpr;
  var itemExtraJoinSql = manualOrderInfo.joinSql;
  var itemBrand = _bv_col_expr("I", "MTRL", ["MTRMARK"], "0");
  var brandName = _bv_col_expr("MK", "MTRMARK", ["NAME"], "''");
  var itemGroup = _bv_col_expr("I", "MTRL", ["MTRGROUP"], "0");
  var groupName = _bv_col_expr("MG", "MTRGROUP", ["NAME"], "''");
  var cat1Id = _bv_col_expr("I", "MTRL", ["CCC88POCAT1"], "0");
  var cat2Id = _bv_col_expr("I", "MTRL", ["CCC88POCAT2"], "0");
  var cat3Id = _bv_col_expr("I", "MTRL", ["CCC88POCAT3"], "0");
  var cat1Name = _bv_col_expr("PC1", "CCC88POCAT1", ["NAME"], "''");
  var cat2Name = _bv_col_expr("PC2", "CCC88POCAT2", ["NAME"], "''");
  var cat3Name = _bv_col_expr("PC3", "CCC88POCAT3", ["NAME"], "''");
  var categoryExternalIdExpr =
    "NULLIF(STUFF((CASE WHEN ISNULL(" + cat1Id + ",0) <> 0 THEN '|' + CAST(" + cat1Id + " AS VARCHAR(20)) ELSE '' END)" +
    " + (CASE WHEN ISNULL(" + cat2Id + ",0) <> 0 THEN '|' + CAST(" + cat2Id + " AS VARCHAR(20)) ELSE '' END)" +
    " + (CASE WHEN ISNULL(" + cat3Id + ",0) <> 0 THEN '|' + CAST(" + cat3Id + " AS VARCHAR(20)) ELSE '' END)" +
    ",1,1,''),'')";
  var categoryNameExpr =
    "NULLIF(STUFF((CASE WHEN ISNULL(" + cat1Name + ",N'') <> N'' THEN N' > ' + " + cat1Name + " ELSE N'' END)" +
    " + (CASE WHEN ISNULL(" + cat2Name + ",N'') <> N'' THEN N' > ' + " + cat2Name + " ELSE N'' END)" +
    " + (CASE WHEN ISNULL(" + cat3Name + ",N'') <> N'' THEN N' > ' + " + cat3Name + " ELSE N'' END)" +
    ",1,3,N''),N'')";
  var hasPurchChannel = _bv_has_column("MTRL", "CCC88ECHANNEL") && _bv_table_exists("CCC88ECHANNEL");
  var channelIdExpr = hasPurchChannel ? "I.CCC88ECHANNEL" : "NULL";
  var channelNameSql = hasPurchChannel ? "CAST(ISNULL(EC.NAME, '') AS VARCHAR(255))" : "CAST('' AS VARCHAR(255))";
  var channelJoinSql = hasPurchChannel ? "LEFT JOIN CCC88ECHANNEL EC ON EC.CCC88ECHANNEL = TRY_CAST(I.CCC88ECHANNEL AS INT) " : "";
  var grossLineExpr = "(ABS(ISNULL(" + lPrice + ",0)) * ABS(ISNULL(" + lQty + ",0)))";
  var discAmtExpr = "(CASE WHEN " + lPrice + " IS NOT NULL AND " + grossLineExpr + " > ABS(ISNULL(" + lNet + ",0))" +
    " THEN " + grossLineExpr + " - ABS(ISNULL(" + lNet + ",0)) ELSE 0 END)";
  var purchaseSign = "(CASE WHEN ISNULL(F.TFPRMS,0) IN (151,152) THEN -1 ELSE 1 END)";
  var docNetTotal = _bv_col_expr("F", "FINDOC", ["NETAMNT"], c.sumAmount);
  var docExpensesTotal = _bv_col_expr("F", "FINDOC", ["EXPN", "TEXPN", "LEXPN"], "0");
  var docGrossTotal = _bv_col_expr("F", "FINDOC", ["SUMAMNT"], "(ISNULL(" + docNetTotal + ",0) + ISNULL(" + docExpensesTotal + ",0) + ISNULL(" + c.taxAmount + ",0))");
  var expenseLikeSeriesExpr =
    "((ISNULL(" + c.sosource + ",0)=1253 AND ISNULL(F.TFPRMS,0)=101)" +
    " OR ISNULL(" + c.series + ",0) IN (1001,1002,1003,1006,1007,1009,1102,3201)" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%Δαπαν%'" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%παροχής υπηρεσι%'" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%Αγοράς Παγίων%'" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%Λογαριασμ%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Δαπάν%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Έξοδ%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Πάγι%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Υπηρεσ%')";
  var docNetAbsTotal = "NULLIF(SUM(ABS(ISNULL(" + lNet + ",0))) OVER (PARTITION BY " + c.findoc + "),0)";
  var lineTaxAbs = "ABS(ISNULL(" + lVat + ",0))";
  var lineTaxDocAbsTotal = "SUM(" + lineTaxAbs + ") OVER (PARTITION BY " + c.findoc + ")";
  var headerTaxAbs = "ABS(ISNULL(" + c.taxAmount + ",0))";
  var vatExpr =
    "(CASE WHEN " +
    lineTaxDocAbsTotal +
    " > 0 THEN " +
    lineTaxAbs +
    " WHEN " +
    docNetAbsTotal +
    " IS NOT NULL THEN (" +
    headerTaxAbs +
    " * ABS(ISNULL(" +
    lNet +
    ",0)) / " +
    docNetAbsTotal +
    ") ELSE 0 END)";
  var grossAbsExpr = "ISNULL(ABS(" + lGross + "), ABS(ISNULL(" + lNet + ",0)) + " + vatExpr + ")";

  var mdWhouse = _bv_col_expr("MD", "MTRDOC", ["WHOUSE"], "0");
  var whName = _bv_col_expr("W", "WHOUSE", ["NAME"], "''");

  var whereSql =
    " WHERE F.COMPANY=" +
    cfg.company +
    " AND ISNULL(" +
    c.sodtype +
    ",0)=12 AND F.SOSOURCE IN (" +
    cfg.purchaseSourceCodes +
    ")" +
    " AND ISNULL(F.TFPRMS,0) IN (102,103,151,152)" +
    " AND NOT " + expenseLikeSeriesExpr;
  whereSql +=
    " AND ISNULL(" + c.finCode + ",'') NOT LIKE N'ΠΑΡ%'" +
    " AND ISNULL(" + c.finCode + ",'') NOT LIKE 'PAR%'" +
    " AND ISNULL(" + seriesInfo.seriesNameExpr + ",'') NOT LIKE N'%Παραγγελ%'" +
    " AND ISNULL(" + seriesInfo.seriesNameExpr + ",'') NOT LIKE '%Order%'";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) + '-' + CAST(ISNULL(" +
    lLineId +
    ",0) AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" +
    c.finCode +
    " AS VARCHAR(128)) AS DOCUMENT_NO," +
    "CAST(" +
    c.series +
    " AS VARCHAR(128)) AS DOCUMENT_SERIES," +
    seriesInfo.seriesNameExpr +
    " AS DOCUMENT_SERIES_NAME," +
    "'purchase_' + CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS VARCHAR(16)) AS DOCUMENT_TYPE," +
    "CONVERT(VARCHAR(10), " +
    c.trnDate +
    ", 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" +
    c.updDate +
    ", " +
    c.trnDate +
    "), 126) AS UPDATED_AT," +
    "CONVERT(VARCHAR(19), " +
    sourceCreatedAt +
    ", 126) AS SOURCE_CREATED_AT," +
    "CAST(ISNULL(" +
    c.branch +
    ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr +
    " AS BRANCH_NAME," +
    "CAST(ISNULL(" +
    c.company +
    ",0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CAST(ISNULL(" +
    mdWhouse +
    ",0) AS VARCHAR(64)) AS WAREHOUSE_EXT_ID," +
    "CAST(ISNULL(" +
    whName +
    ", CAST(ISNULL(" +
    mdWhouse +
    ",0) AS VARCHAR(255))) AS VARCHAR(255)) AS WAREHOUSE_NAME," +
    "CAST(ISNULL(S.CODE, " +
    c.trdr +
    ") AS VARCHAR(128)) AS SUPPLIER_EXT_ID," +
    "CAST(ISNULL(S.NAME, '') AS VARCHAR(255)) AS SUPPLIER_NAME," +
    "CAST(NULLIF(CAST(ISNULL(" + channelIdExpr + ", 0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS CHANNEL_EXT_ID," +
    channelNameSql + " AS CHANNEL_NAME," +
    "CAST(ISNULL(" +
    _bv_col_expr("S", "TRDR", ["AFM"], "''") +
    ", '') AS VARCHAR(64)) AS SUPPLIER_AFM," +
    "CAST(ISNULL(I.CODE, " +
    lMtrl +
    ") AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" +
    iName +
    ", '') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(NULLIF(CAST(ISNULL(" +
    itemBrand +
    ",0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS BRAND_EXTERNAL_ID," +
    "CAST(ISNULL(" +
    brandName +
    ", '') AS VARCHAR(255)) AS BRAND_NAME," +
    "CAST(NULLIF(CAST(ISNULL(" +
    itemGroup +
    ",0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS GROUP_EXTERNAL_ID," +
    "CAST(ISNULL(" +
    groupName +
    ", '') AS VARCHAR(255)) AS GROUP_NAME," +
    "CAST(" +
    categoryExternalIdExpr +
    " AS VARCHAR(64)) AS CATEGORY_EXTERNAL_ID," +
    "CAST(" +
    categoryNameExpr +
    " AS VARCHAR(255)) AS CATEGORY_NAME," +
    "CAST(ISNULL(" +
    manualOrderCategoryExpr +
    ", '') AS VARCHAR(128)) AS MANUAL_ORDER_CATEGORY," +
    "CAST(ISNULL(" +
    commercialStatusExpr +
    ", '') AS VARCHAR(128)) AS COMMERCIAL_STATUS," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    lQty +
    ",0)) AS FLOAT) AS QTY," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    lNet +
    ",0)) AS FLOAT) AS NET_VALUE," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    lLineValue +
    ",0)) AS FLOAT) AS LINE_VALUE," +
    "CAST(" +
    purchaseSign +
    " * (" +
    grossAbsExpr +
    ")" +
    " AS FLOAT) AS GROSS_VALUE," +
    "CAST(" +
    purchaseSign +
    " * (" +
    vatExpr +
    ")" +
    " AS FLOAT) AS VAT_AMOUNT," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    docNetTotal +
    ",0)) AS FLOAT) AS DOC_NET_TOTAL," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    docExpensesTotal +
    ",0)) AS FLOAT) AS DOC_EXPENSES_TOTAL," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    c.taxAmount +
    ",0)) AS FLOAT) AS DOC_TAX_TOTAL," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    docGrossTotal +
    ",0)) AS FLOAT) AS DOC_GROSS_TOTAL," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    lNoDiscount +
    ",0)) AS FLOAT) AS NO_DISCOUNT_AMOUNT," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    lNoDiscount +
    ",0)) AS FLOAT) AS NODSCAMNT," +
    "CAST(" +
    purchaseSign +
    " * ABS(ISNULL(" +
    lCost +
    ",0)) AS FLOAT) AS COST_AMOUNT," +
    "CAST(" +
    purchaseSign +
    " * (" + discAmtExpr + ") AS FLOAT) AS DISCOUNT_AMOUNT," +
    "CAST(ABS(ISNULL(" + disc1Val + ",0)) AS FLOAT) AS DISCOUNT1_AMOUNT," +
    "CAST(ABS(ISNULL(" + disc2Val + ",0)) AS FLOAT) AS DISCOUNT2_AMOUNT," +
    "CAST(ABS(ISNULL(" + disc3Val + ",0)) AS FLOAT) AS DISCOUNT3_AMOUNT," +
    "CAST(ABS(ISNULL(" + disc1Pct + ",0) + ISNULL(" + disc2Pct + ",0) + ISNULL(" + disc3Pct + ",0)) AS FLOAT) AS DISCOUNT_PCT," +
    "CAST(ISNULL(" + disc1Pct + ",NULL) AS FLOAT) AS DISCOUNT1_PCT," +
    "CAST(ISNULL(" + disc2Pct + ",NULL) AS FLOAT) AS DISCOUNT2_PCT," +
    "CAST(ISNULL(" + disc3Pct + ",NULL) AS FLOAT) AS DISCOUNT3_PCT," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(ISNULL(" +
    c.soredir +
    ",0) AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(ISNULL(" +
    c.sodtype +
    ",0) AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(F.TFPRMS,0) AS INT) AS DOCUMENT_BEHAVIOR_CODE," +
    "CASE WHEN ISNULL(F.TFPRMS,0) IN (151,152) THEN 'credit_or_return' ELSE 'purchase' END AS PURCHASE_FLOW," +
    "CASE WHEN ISNULL(F.TFPRMS,0) IN (151,152) THEN 1 ELSE 0 END AS IS_CREDIT_OR_RETURN," +
    "CONVERT(VARCHAR(10), " +
    c.dueDate +
    ", 23) AS DUE_DATE," +
    "CAST(DATEDIFF(day, " +
    c.trnDate +
    ", " +
    c.dueDate +
    ") AS INT) AS PAYMENT_TERMS_DAYS," +
    paymentExpr +
    " AS PAYMENT_METHOD," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS INT) AS OBJECT_ID " +
    "FROM FINDOC F " +
    "INNER JOIN MTRLINES L ON L.FINDOC=" +
    c.findoc +
    " AND L.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRDOC MD ON MD.FINDOC=" +
    c.findoc +
    " AND MD.COMPANY=F.COMPANY " +
    "LEFT JOIN WHOUSE W ON W.WHOUSE=" +
    mdWhouse +
    " AND W.COMPANY=F.COMPANY " +
    "LEFT JOIN TRDR S ON S.TRDR=" +
    c.trdr +
    " AND S.COMPANY=F.COMPANY " +
    "OUTER APPLY (SELECT TOP 1 P.CODE, P.NAME FROM PAYMENT P WHERE P.PAYMENT=" +
    docPayment +
    " AND P.SODTYPE=" +
    c.sodtype +
    " AND (P.COMPANY=F.COMPANY OR P.COMPANY=1000) ORDER BY CASE WHEN P.COMPANY=F.COMPANY THEN 0 ELSE 1 END) PM " +
    "LEFT JOIN MTRL I ON I.MTRL=" +
    lMtrl +
    " AND I.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRMARK MK ON MK.MTRMARK=I.MTRMARK AND MK.COMPANY=I.COMPANY " +
    "LEFT JOIN MTRGROUP MG ON MG.MTRGROUP=I.MTRGROUP AND MG.COMPANY=I.COMPANY " +
    "LEFT JOIN CCC88POCAT1 PC1 ON PC1.CCC88POCAT1=I.CCC88POCAT1 " +
    "LEFT JOIN CCC88POCAT2 PC2 ON PC2.CCC88POCAT2=I.CCC88POCAT2 " +
    "LEFT JOIN CCC88POCAT3 PC3 ON PC3.CCC88POCAT3=I.CCC88POCAT3 " +
    channelJoinSql +
    itemExtraJoinSql +
    branchInfo.joinSql +
    seriesInfo.joinSql +
    whereSql +
    " ORDER BY " +
    c.trnDate +
    " ASC, " +
    c.findoc +
    " ASC, " +
    lLineId +
    " ASC";
  return sql;
}

function _bv_supplier_orders_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var seriesInfo = _bv_series_info_expr(c);
  var branchInfo = _bv_branch_info_expr(c);
  var lLineId = _bv_col_expr("L", "MTRLINES", ["MTRLINES", "LINENUM"], "0");
  var lMtrl = _bv_col_expr("L", "MTRLINES", ["MTRL"], "0");
  var lQty = _bv_col_expr("L", "MTRLINES", ["QTY1", "QTY"], "0");
  var lCoveredQty = _bv_col_expr("L", "MTRLINES", ["QTY1COV"], "0");
  var lCancelledQty = _bv_col_expr("L", "MTRLINES", ["QTY1CANC"], "0");
  var lValue = _bv_col_expr("L", "MTRLINES", ["LINEVAL", "NETLINEVAL", "NETVAL", "NETAMNT"], "0");
  var supplierCode = _bv_col_expr("SUP", "TRDR", ["CODE"], c.trdr);
  var supplierName = _bv_col_expr("SUP", "TRDR", ["NAME"], "''");
  var supplierAfm = _bv_col_expr("SUP", "TRDR", ["AFM", "VATREGNO"], "''");
  var iName = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var hasTransformation =
    "(CASE WHEN ISNULL(" + _bv_col_expr("F", "FINDOC", ["FULLYTRANSF"], "0") + ",0)=1 THEN 1 " +
    "WHEN EXISTS (SELECT 1 FROM MTRLINES T WHERE T.COMPANY=L.COMPANY AND T.FINDOCS=" + c.findoc + ") THEN 1 ELSE 0 END)";
  var whereSql =
    " WHERE F.COMPANY=" + cfg.company +
    " AND ISNULL(" + c.sosource + ",0)=1251" +
    " AND ISNULL(" + c.sodtype + ",0)=12" +
    " AND ISNULL(" + c.tfprms + ",0)=201" +
    " AND ISNULL(" + c.series + ",0) IN (2021,2031)" +
    " AND ISNULL(" + _bv_col_expr("F", "FINDOC", ["ISCANCEL"], "0") + ",0)=0";
  if (cfg.fromDate) whereSql += " AND " + c.trnDate + " >= '" + cfg.fromDate + "'";
  if (cfg.toDate) whereSql += " AND " + c.trnDate + " < DATEADD(day, 1, '" + cfg.toDate + "')";

  return "SELECT " + _bv_top_clause(cfg.limit) +
    "CAST(" + c.findoc + " AS VARCHAR(40)) + '-' + CAST(ISNULL(" + lLineId + ",0) AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" + c.findoc + " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" + c.finCode + " AS VARCHAR(128)) AS DOCUMENT_NO," +
    "CAST(" + c.series + " AS VARCHAR(128)) AS DOCUMENT_SERIES," +
    seriesInfo.seriesNameExpr + " AS DOCUMENT_SERIES_NAME," +
    "CONVERT(VARCHAR(10), " + c.trnDate + ", 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" + c.updDate + ", " + c.trnDate + "), 126) AS UPDATED_AT," +
    "CAST(ISNULL(" + c.tfprms + ",0) AS INT) AS DOCUMENT_BEHAVIOR_CODE," +
    "CAST(ISNULL(" + c.branch + ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr + " AS BRANCH_NAME," +
    "CAST(ISNULL(" + supplierCode + ", " + c.trdr + ") AS VARCHAR(128)) AS SUPPLIER_EXT_ID," +
    "CAST(ISNULL(" + supplierName + ", '') AS VARCHAR(255)) AS SUPPLIER_NAME," +
    "CAST(ISNULL(" + supplierAfm + ", '') AS VARCHAR(64)) AS SUPPLIER_AFM," +
    "CAST(ISNULL(I.CODE, " + lMtrl + ") AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" + iName + ", '') AS VARCHAR(512)) AS ITEM_NAME," +
    "CAST(ISNULL(" + lQty + ",0) AS FLOAT) AS ORDER_QTY," +
    "CAST(ISNULL(" + lCoveredQty + ",0) AS FLOAT) AS COVERED_QTY," +
    "CAST(ISNULL(" + lCancelledQty + ",0) AS FLOAT) AS CANCELLED_QTY," +
    "CAST(ISNULL(" + lValue + ",0) AS FLOAT) AS LINE_VALUE," +
    "CAST(" + hasTransformation + " AS INT) AS HAS_TRANSFORMATION," +
    "CASE WHEN " + hasTransformation + "=1 THEN 'closed' ELSE 'open' END AS ORDER_STATUS " +
    "FROM FINDOC F " +
    "INNER JOIN MTRLINES L ON L.FINDOC=" + c.findoc + " AND L.COMPANY=F.COMPANY " +
    "LEFT JOIN TRDR SUP ON SUP.TRDR=" + c.trdr + " AND SUP.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRL I ON I.MTRL=" + lMtrl + " AND I.COMPANY=F.COMPANY " +
    branchInfo.joinSql +
    seriesInfo.joinSql +
    whereSql +
    " ORDER BY " + c.trnDate + " ASC, " + c.findoc + " ASC, " + lLineId + " ASC";
}

function _bv_inventory_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var seriesInfo = _bv_series_info_expr(c);
  var branchInfo = _bv_branch_info_expr(c);

  var lLineId = _bv_col_expr("L", "MTRLINES", ["MTRLINES", "LINENUM"], "0");
  var lMtrl = _bv_col_expr("L", "MTRLINES", ["MTRL"], "0");
  var iName = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var iSoType = _bv_col_expr("I", "MTRL", ["SODTYPE", "SOTYPE"], "0");
  var iCat1 = _bv_col_expr("PC1", "CCC88POCAT1", ["NAME"], "''");
  var iCat2 = _bv_col_expr("PC2", "CCC88POCAT2", ["NAME"], "''");
  var iCat3 = _bv_col_expr("PC3", "CCC88POCAT3", ["NAME"], "''");
  var iBrand = _bv_col_expr("I", "MTRL", ["MTRMARK"], "NULL");
  var iManufacturer = _bv_col_expr("I", "MTRL", ["MTRMANFCTR"], "NULL");
  var iAltBarcodeExpr =
    "NULLIF(STUFF((SELECT ',' + CAST(MS.CODE AS VARCHAR(128)) " +
    "FROM MTRSUBSTITUTE MS " +
    "WHERE MS.COMPANY = I.COMPANY AND MS.MTRL = I.MTRL AND NULLIF(ISNULL(MS.CODE,''),'') IS NOT NULL " +
    "FOR XML PATH(''), TYPE).value('.', 'varchar(max)'), 1, 1, ''), '')";
  var lQty = _bv_col_expr("L", "MTRLINES", ["QTY1", "QTY"], "0");
  var lVal = _bv_col_expr("L", "MTRLINES", ["LINEVAL", "NETLINEVAL", "NETVAL", "NETAMNT"], "0");
  var lCost = _bv_col_expr("L", "MTRLINES", ["SALESCVAL", "COSTVAL", "COSTVALUE", "LCOST", "COST"], lVal);
  var mdWhouse = _bv_col_expr("MD", "MTRDOC", ["WHOUSE"], "0");
  var activeExpr = _bv_col_expr("I", "MTRL", ["ISACTIVE"], "1");
  var vatExpr = _bv_col_expr("I", "MTRL", ["VAT"], "NULL");
  var manualOrderInfo = _bv_item_manual_order_info("I");
  var manualOrderCategoryExpr = manualOrderInfo.expr;
  var commercialStatusExpr = manualOrderInfo.commercialStatusExpr;
  var itemExtraJoinSql = manualOrderInfo.joinSql;
  var replInfo = _bv_item_replenishment_info("I");
  var itemReplJoinSql = replInfo.joinSql;
  // On-hand is the cumulative net over ALL fiscal periods (PERIOD 0 opening + 1..12),
  // so reserved/expected are summed inside the snapshot subquery (MB alias) and surfaced
  // as S.RESQTY1 / S.EXPCTQTY1.
  var stockReservedRaw = _bv_col_expr("MB", "MTRBALSHEET", ["RESQTY1", "RESERVEDQTY", "RESQTY", "COMMITQTY1"], "0");
  var stockExpectedRaw = _bv_col_expr("MB", "MTRBALSHEET", ["EXPCTQTY1", "EXPECTEDQTY", "ONORDERQTY", "ORDEREDQTY1"], "0");
  var stockReservedExpr = "S.RESQTY1";
  var stockExpectedExpr = "S.EXPCTQTY1";
  var branchLimitSnapshotJoinSql = "";
  var branchLimitAdjustmentJoinSql = "";
  var minStockExpr = replInfo.minStockExpr;
  var replMoqExpr = replInfo.replMoqExpr;
  if (_bv_table_exists("MTRBRNLIMITS")) {
    var blMtrlExpr = _bv_col_expr("RBL", "MTRBRNLIMITS", ["MTRL"], "NULL");
    var blCompanyExpr = _bv_col_expr("RBL", "MTRBRNLIMITS", ["COMPANY"], "I.COMPANY");
    var blBranchExpr = _bv_col_expr("RBL", "MTRBRNLIMITS", ["BRANCH"], "NULL");
    var blMinStockExpr = _bv_col_expr("RBL", "MTRBRNLIMITS", ["REMAINLIMMIN"], "NULL");
    var blReplMoqExpr = _bv_col_expr("RBL", "MTRBRNLIMITS", ["REORDERLEVEL"], "NULL");
    var snapshotBranchExpr = "ISNULL(BR.BRANCH, ISNULL(NULLIF(W.WHOUSEG,0), ISNULL(S.WHOUSE,0)))";
    branchLimitSnapshotJoinSql =
      "LEFT JOIN MTRBRNLIMITS RBL ON " +
      blMtrlExpr +
      "=I.MTRL AND " +
      blCompanyExpr +
      "=I.COMPANY AND " +
      blBranchExpr +
      "=" +
      snapshotBranchExpr +
      " ";
    branchLimitAdjustmentJoinSql =
      "LEFT JOIN MTRBRNLIMITS RBL ON " +
      blMtrlExpr +
      "=I.MTRL AND " +
      blCompanyExpr +
      "=I.COMPANY AND " +
      blBranchExpr +
      "=" +
      c.branch +
      " ";
    minStockExpr = "COALESCE(" + blMinStockExpr + ", " + minStockExpr + ")";
    replMoqExpr = "COALESCE(" + blReplMoqExpr + ", " + replMoqExpr + ")";
  }
  var snapshotAsOf = cfg.toDate !== "" ? _bv_sql_quote(cfg.toDate) : "GETDATE()";

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.inventorySourceCodes + ")";
  whereSql += " AND ISNULL(" + iSoType + ",0)=" + cfg.inventoryItemSoType;
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var adjustmentWhereSql = " WHERE F.COMPANY=" + cfg.company;
  adjustmentWhereSql += " AND F.SOSOURCE IN (" + cfg.purchaseSourceCodes + ")";
  adjustmentWhereSql += " AND ISNULL(" + iSoType + ",0)=" + cfg.inventoryItemSoType;
  adjustmentWhereSql +=
    " AND (" +
    "ISNULL(" + c.series + ",0)=1006" +
    " OR ISNULL(" + c.tfprms + ",0) IN (101,154,301)" +
    ")";
  if (cfg.fromDate !== "") adjustmentWhereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") adjustmentWhereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var snapshotSql =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST('IS|' + CAST(ISNULL(S.COMPANY,0) AS VARCHAR(32)) + '|' + CAST(ISNULL(S.FISCPRD,0) AS VARCHAR(16)) + '|' + CAST(ISNULL(S.WHOUSE,0) AS VARCHAR(32)) + '|' + CAST(ISNULL(S.MTRL,0) AS VARCHAR(40)) AS VARCHAR(128)) AS EVENT_ID," +
    "CAST('INVSNAP-' + CAST(ISNULL(S.FISCPRD,0) AS VARCHAR(16)) AS VARCHAR(128)) AS DOCUMENT_ID," +
    "CAST('Inventory Snapshot ' + CAST(ISNULL(S.FISCPRD,0) AS VARCHAR(16)) AS VARCHAR(128)) AS DOCUMENT_NO," +
    "CAST('SNAPSHOT' AS VARCHAR(128)) AS DOCUMENT_SERIES," +
    "CAST('Υπόλοιπο Αποθέματος' AS VARCHAR(255)) AS DOCUMENT_SERIES_NAME," +
    "CAST('Υπόλοιπο Αποθέματος' AS VARCHAR(255)) AS DOCUMENT_TYPE," +
    "CONVERT(VARCHAR(10), CAST(" + snapshotAsOf + " AS DATE), 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), CAST(" + snapshotAsOf + " AS DATETIME), 126) AS UPDATED_AT," +
    "CAST(CAST(ISNULL(S.COMPANY,0) AS VARCHAR(32)) + ':' + CAST(ISNULL(BR.BRANCH, ISNULL(NULLIF(W.WHOUSEG,0), ISNULL(S.WHOUSE,0))) AS VARCHAR(32)) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    "CAST(ISNULL(BR.NAME, CAST(ISNULL(NULLIF(W.WHOUSEG,0), ISNULL(S.WHOUSE,0)) AS VARCHAR(255))) AS VARCHAR(255)) AS BRANCH_NAME," +
    "CAST(ISNULL(S.COMPANY,0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CAST(ISNULL(S.WHOUSE,0) AS VARCHAR(64)) AS WAREHOUSE_EXT_ID," +
    "CAST(ISNULL(W.NAME, CAST(ISNULL(S.WHOUSE,0) AS VARCHAR(255))) AS VARCHAR(255)) AS WAREHOUSE_NAME," +
    "CAST(ISNULL(I.CODE, S.MTRL) AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(I.SODTYPE,0) AS INT) AS SOFTONE_SOTYPE," +
    "CAST(ISNULL(I.NAME,'') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(NULLIF(ISNULL(I.CODE1,''),'') AS VARCHAR(128)) AS BARCODE," +
    "CAST(" + iAltBarcodeExpr + " AS VARCHAR(1024)) AS ALTERNATE_BARCODES," +
    "CAST(I.VAT AS DECIMAL(18,4)) AS VAT_RATE," +
    "CAST(ISNULL(VT.NAME,'') AS VARCHAR(255)) AS VAT_LABEL," +
    "CAST(ISNULL(PC1.NAME,'') AS VARCHAR(255)) AS CATEGORY_1," +
    "CAST(ISNULL(PC2.NAME,'') AS VARCHAR(255)) AS CATEGORY_2," +
    "CAST(ISNULL(PC3.NAME,'') AS VARCHAR(255)) AS CATEGORY_3," +
    "CAST(ISNULL(" + manualOrderCategoryExpr + ", '') AS VARCHAR(128)) AS MANUAL_ORDER_CATEGORY," +
    "CAST(ISNULL(" + commercialStatusExpr + ", '') AS VARCHAR(128)) AS COMMERCIAL_STATUS," +
    "CAST(NULLIF(CAST(ISNULL(I.MTRMARK,0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS BRAND_EXTERNAL_ID," +
    "CAST(ISNULL(MK.NAME,'') AS VARCHAR(255)) AS BRAND_NAME," +
    "CAST(NULLIF(CAST(ISNULL(I.MTRGROUP,0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS GROUP_EXTERNAL_ID," +
    "CAST(ISNULL(MG.NAME,'') AS VARCHAR(255)) AS GROUP_NAME," +
    "CAST(ISNULL(CG.NAME,'') AS VARCHAR(255)) AS COMMERCIAL_CATEGORY," +
    "CAST(NULLIF(CAST(ISNULL(I.MTRMANFCTR,0) AS VARCHAR(128)), '0') AS VARCHAR(128)) AS MANUFACTURER_CODE," +
    "CAST(ISNULL(MF.NAME,'') AS VARCHAR(255)) AS MANUFACTURER_NAME," +
    "CAST(ISNULL(S.IMPQTY1,0)-ISNULL(S.EXPQTY1,0) AS FLOAT) AS QTY," +
    "CAST(ISNULL(S.IMPQTY1,0)-ISNULL(S.EXPQTY1,0) AS FLOAT) AS QTY_ON_HAND," +
    "CAST(ISNULL(" + stockReservedExpr + ",0) AS FLOAT) AS QTY_RESERVED," +
    "CAST(ISNULL(" + stockExpectedExpr + ",0) AS FLOAT) AS QTY_EXPECTED," +
    "CAST((ISNULL(S.IMPQTY1,0)-ISNULL(S.EXPQTY1,0))-ISNULL(" + stockReservedExpr + ",0) AS FLOAT) AS QTY_AVAILABLE," +
    "CAST((ISNULL(S.IMPQTY1,0)-ISNULL(S.EXPQTY1,0)) * ISNULL(MAC.UNIT_COST,0) AS FLOAT) AS COST_AMOUNT," +
    "CAST((ISNULL(S.IMPQTY1,0)-ISNULL(S.EXPQTY1,0)) * ISNULL(I.PRICEW,0) AS FLOAT) AS VALUE_AMOUNT," +
    "CAST((ISNULL(S.IMPQTY1,0)-ISNULL(S.EXPQTY1,0)) * ISNULL(I.PRICER,0) AS FLOAT) AS RETAIL_VALUE_AMOUNT," +
    "CAST(ISNULL(S.IMPVAL,0)-ISNULL(S.EXPVAL,0) AS FLOAT) AS FINANCIAL_VALUE_AMOUNT," +
    "CAST(ISNULL(I.ISACTIVE,1) AS INT) AS IS_ACTIVE_SOURCE," +
    "CAST(ISNULL(" + replInfo.status1Expr + ", '') AS VARCHAR(64)) AS REPLENISHMENT_STATUS_1," +
    "CAST(ISNULL(" + replInfo.status2Expr + ", '') AS VARCHAR(128)) AS REPLENISHMENT_STATUS_2," +
    "CAST(" + minStockExpr + " AS FLOAT) AS MIN_STOCK," +
    "CAST(" + replMoqExpr + " AS FLOAT) AS REPLENISHMENT_MOQ," +
    "CAST(" + replInfo.vendorMoqExpr + " AS FLOAT) AS VENDOR_MOQ," +
    "CAST(" + replInfo.purchasePriceExpr + " AS FLOAT) AS CURRENT_PURCHASE_PRICE," +
    "CAST('snapshot' AS VARCHAR(32)) AS MOVEMENT_TYPE," +
    "CAST(ISNULL(S.FISCPRD,0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(0 AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(12 AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(S.FISCPRD,0) AS INT) AS OBJECT_ID " +
    "FROM (SELECT MB.COMPANY, MB.FISCPRD, MB.MTRL, MB.WHOUSE, " +
      "SUM(ISNULL(MB.IMPQTY1,0)) AS IMPQTY1, SUM(ISNULL(MB.EXPQTY1,0)) AS EXPQTY1, " +
      "SUM(ISNULL(MB.IMPVAL,0)) AS IMPVAL, SUM(ISNULL(MB.EXPVAL,0)) AS EXPVAL, " +
      "SUM(ISNULL(" + stockReservedRaw + ",0)) AS RESQTY1, SUM(ISNULL(" + stockExpectedRaw + ",0)) AS EXPCTQTY1 " +
      "FROM MTRBALSHEET MB WHERE MB.COMPANY=" + cfg.company + " GROUP BY MB.COMPANY, MB.FISCPRD, MB.MTRL, MB.WHOUSE) S " +
    "INNER JOIN (SELECT COMPANY, MAX(FISCPRD) AS FISCPRD FROM MTRBALSHEET WHERE COMPANY=" + cfg.company + " AND PERIOD=0 GROUP BY COMPANY) SP ON SP.COMPANY=S.COMPANY AND SP.FISCPRD=S.FISCPRD " +
    "INNER JOIN (SELECT COMPANY, FISCPRD, MTRL FROM MTRBALSHEET WHERE COMPANY=" + cfg.company + " GROUP BY COMPANY, FISCPRD, MTRL HAVING ABS(SUM(ISNULL(IMPQTY1,0)-ISNULL(EXPQTY1,0)))>0.0001) ITEM_STOCK ON ITEM_STOCK.COMPANY=S.COMPANY AND ITEM_STOCK.FISCPRD=S.FISCPRD AND ITEM_STOCK.MTRL=S.MTRL " +
    "LEFT JOIN (SELECT COMPANY, MTRL, CASE WHEN SUM(ISNULL(PURQTY,0))>0 THEN SUM(ISNULL(PURVAL,0))/SUM(ISNULL(PURQTY,0)) ELSE 0 END AS UNIT_COST FROM MTRBALSHEET WHERE COMPANY=" + cfg.company + " GROUP BY COMPANY, MTRL) MAC ON MAC.COMPANY=S.COMPANY AND MAC.MTRL=S.MTRL " +
    "LEFT JOIN WHOUSE W ON W.WHOUSE=S.WHOUSE AND W.COMPANY=S.COMPANY " +
    "LEFT JOIN BRANCH BR ON BR.BRANCH=ISNULL(NULLIF(W.WHOUSEG,0), ISNULL(S.WHOUSE,0)) AND BR.COMPANY=S.COMPANY " +
    "LEFT JOIN MTRL I ON I.MTRL=S.MTRL AND I.COMPANY=S.COMPANY " +
    itemExtraJoinSql +
    itemReplJoinSql +
    branchLimitSnapshotJoinSql +
    "LEFT JOIN VAT VT ON VT.VAT=I.VAT " +
    "LEFT JOIN MTRMARK MK ON MK.MTRMARK=I.MTRMARK AND MK.COMPANY=I.COMPANY " +
    "LEFT JOIN MTRMANFCTR MF ON MF.MTRMANFCTR=I.MTRMANFCTR AND MF.COMPANY=I.COMPANY " +
    "LEFT JOIN CCC88POCAT1 PC1 ON PC1.CCC88POCAT1=I.CCC88POCAT1 " +
    "LEFT JOIN CCC88POCAT2 PC2 ON PC2.CCC88POCAT2=I.CCC88POCAT2 " +
    "LEFT JOIN CCC88POCAT3 PC3 ON PC3.CCC88POCAT3=I.CCC88POCAT3 " +
    "LEFT JOIN MTRGROUP MG ON MG.MTRGROUP=I.MTRGROUP AND MG.COMPANY=I.COMPANY " +
    "LEFT JOIN MTRPCATEGORY CG ON CG.MTRPCATEGORY=I.MTRPCATEGORY AND CG.COMPANY=I.COMPANY " +
    "WHERE S.COMPANY=" + cfg.company + " AND ISNULL(W.ISACTIVE,1)=1 AND ISNULL(I.SODTYPE,0)=" + cfg.inventoryItemSoType +
    " AND ABS(ISNULL(S.IMPQTY1,0)-ISNULL(S.EXPQTY1,0))>0.0001";

  var adjustmentSql =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) + '-' + CAST(ISNULL(" +
    lLineId +
    ",0) AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" +
    c.finCode +
    " AS VARCHAR(128)) AS DOCUMENT_NO," +
    "CAST(" +
    c.series +
    " AS VARCHAR(128)) AS DOCUMENT_SERIES," +
    seriesInfo.seriesNameExpr +
    " AS DOCUMENT_SERIES_NAME," +
    "CAST(ISNULL(" +
    seriesInfo.seriesNameExpr +
    ",'Κίνηση Αποθήκης') AS VARCHAR(255)) AS DOCUMENT_TYPE," +
    "CONVERT(VARCHAR(10), " +
    c.trnDate +
    ", 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" +
    c.updDate +
    ", " +
    c.trnDate +
    "), 126) AS UPDATED_AT," +
    "CAST(ISNULL(" +
    c.branch +
    ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr +
    " AS BRANCH_NAME," +
    "CAST(ISNULL(" +
    c.company +
    ",0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CAST(ISNULL(" +
    mdWhouse +
    ",0) AS VARCHAR(64)) AS WAREHOUSE_EXT_ID," +
    "CAST(ISNULL(W.NAME, CAST(ISNULL(" +
    mdWhouse +
    ",0) AS VARCHAR(255))) AS VARCHAR(255)) AS WAREHOUSE_NAME," +
    "CAST(ISNULL(I.CODE, " +
    lMtrl +
    ") AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" +
    iSoType +
    ",0) AS INT) AS SOFTONE_SOTYPE," +
    "CAST(ISNULL(" +
    iName +
    ", '') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(NULLIF(ISNULL(I.CODE1, ''), '') AS VARCHAR(128)) AS BARCODE," +
    "CAST(" +
    iAltBarcodeExpr +
    " AS VARCHAR(1024)) AS ALTERNATE_BARCODES," +
    "CAST(" +
    vatExpr +
    " AS DECIMAL(18,4)) AS VAT_RATE," +
    "CAST(ISNULL(VT.NAME, '') AS VARCHAR(255)) AS VAT_LABEL," +
    "CAST(ISNULL(" +
    iCat1 +
    ", '') AS VARCHAR(255)) AS CATEGORY_1," +
    "CAST(ISNULL(" +
    iCat2 +
    ", '') AS VARCHAR(255)) AS CATEGORY_2," +
    "CAST(ISNULL(" +
    iCat3 +
    ", '') AS VARCHAR(255)) AS CATEGORY_3," +
    "CAST(ISNULL(" +
    manualOrderCategoryExpr +
    ", '') AS VARCHAR(128)) AS MANUAL_ORDER_CATEGORY," +
    "CAST(ISNULL(" +
    commercialStatusExpr +
    ", '') AS VARCHAR(128)) AS COMMERCIAL_STATUS," +
    "CAST(NULLIF(CAST(ISNULL(" +
    iBrand +
    ",0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS BRAND_EXTERNAL_ID," +
    "CAST(ISNULL(MK.NAME, '') AS VARCHAR(255)) AS BRAND_NAME," +
    "CAST(NULLIF(CAST(ISNULL(I.MTRGROUP,0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS GROUP_EXTERNAL_ID," +
    "CAST(ISNULL(MG.NAME, '') AS VARCHAR(255)) AS GROUP_NAME," +
    "CAST(ISNULL(CG.NAME, '') AS VARCHAR(255)) AS COMMERCIAL_CATEGORY," +
    "CAST(NULLIF(CAST(ISNULL(" +
    iManufacturer +
    ",0) AS VARCHAR(128)), '0') AS VARCHAR(128)) AS MANUFACTURER_CODE," +
    "CAST(ISNULL(MF.NAME, '') AS VARCHAR(255)) AS MANUFACTURER_NAME," +
    "CAST(ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS QTY," +
    "CAST(ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS QTY_ON_HAND," +
    "CAST(0 AS FLOAT) AS QTY_RESERVED," +
    "CAST(0 AS FLOAT) AS QTY_EXPECTED," +
    "CAST(ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS QTY_AVAILABLE," +
    "CAST(ISNULL(" +
    lCost +
    ",0) AS FLOAT) AS COST_AMOUNT," +
    "CAST(ISNULL(" +
    lVal +
    ",0) AS FLOAT) AS VALUE_AMOUNT," +
    "CAST(ISNULL(" +
    _bv_col_expr("L", "MTRLINES", ["PRICE", "PRICEM"], lVal) +
    ",0) * ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS RETAIL_VALUE_AMOUNT," +
    "CAST(ISNULL(" +
    lCost +
    ",0) AS FLOAT) AS FINANCIAL_VALUE_AMOUNT," +
    "CAST(ISNULL(" +
    activeExpr +
    ",1) AS INT) AS IS_ACTIVE_SOURCE," +
    "CAST(ISNULL(" + replInfo.status1Expr + ", '') AS VARCHAR(64)) AS REPLENISHMENT_STATUS_1," +
    "CAST(ISNULL(" + replInfo.status2Expr + ", '') AS VARCHAR(128)) AS REPLENISHMENT_STATUS_2," +
    "CAST(" + minStockExpr + " AS FLOAT) AS MIN_STOCK," +
    "CAST(" + replMoqExpr + " AS FLOAT) AS REPLENISHMENT_MOQ," +
    "CAST(" + replInfo.vendorMoqExpr + " AS FLOAT) AS VENDOR_MOQ," +
    "CAST(" + replInfo.purchasePriceExpr + " AS FLOAT) AS CURRENT_PURCHASE_PRICE," +
    "CASE WHEN ISNULL(" +
    lQty +
    ",0) >= 0 THEN 'entry' ELSE 'exit' END AS MOVEMENT_TYPE," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(ISNULL(" +
    c.soredir +
    ",0) AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(ISNULL(" +
    c.sodtype +
    ",0) AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS INT) AS OBJECT_ID " +
    "FROM FINDOC F " +
    "INNER JOIN MTRLINES L ON L.FINDOC=" +
    c.findoc +
    " AND L.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRDOC MD ON MD.FINDOC=" +
    c.findoc +
    " AND MD.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRL I ON I.MTRL=" +
    lMtrl +
    " AND I.COMPANY=F.COMPANY " +
    itemExtraJoinSql +
    itemReplJoinSql +
    branchLimitAdjustmentJoinSql +
    "LEFT JOIN VAT VT ON VT.VAT = I.VAT " +
    "LEFT JOIN MTRMARK MK ON MK.MTRMARK = I.MTRMARK AND MK.COMPANY = I.COMPANY " +
    "LEFT JOIN MTRMANFCTR MF ON MF.MTRMANFCTR = I.MTRMANFCTR AND MF.COMPANY = I.COMPANY " +
    "LEFT JOIN CCC88POCAT1 PC1 ON PC1.CCC88POCAT1 = I.CCC88POCAT1 " +
    "LEFT JOIN CCC88POCAT2 PC2 ON PC2.CCC88POCAT2 = I.CCC88POCAT2 " +
    "LEFT JOIN CCC88POCAT3 PC3 ON PC3.CCC88POCAT3 = I.CCC88POCAT3 " +
    "LEFT JOIN WHOUSE W ON W.WHOUSE=" +
    mdWhouse +
    " AND W.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRGROUP MG ON MG.MTRGROUP = I.MTRGROUP AND MG.COMPANY = I.COMPANY " +
    "LEFT JOIN MTRPCATEGORY CG ON CG.MTRPCATEGORY = I.MTRPCATEGORY AND CG.COMPANY = I.COMPANY " +
    branchInfo.joinSql +
    seriesInfo.joinSql +
    adjustmentWhereSql;

  return "SELECT * FROM (" + snapshotSql + " UNION ALL " + adjustmentSql + ") INV ORDER BY DOC_DATE ASC, DOCUMENT_ID ASC, EVENT_ID ASC";
}

function _bv_item_master_sql(cfg) {
  var codeExpr = _bv_col_expr("I", "MTRL", ["CODE"], "CAST(I.MTRL AS VARCHAR(128))");
  var nameExpr = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var updatedAtExpr = _bv_col_expr("I", "MTRL", ["UPDDATE", "INSDATE"], "GETDATE()");
  var barcodeExpr = _bv_col_expr("I", "MTRL", ["CODE1"], "''");
  var altBarcodeExpr =
    "NULLIF(STUFF((SELECT ',' + CAST(MS.CODE AS VARCHAR(128)) " +
    "FROM MTRSUBSTITUTE MS " +
    "WHERE MS.COMPANY = I.COMPANY AND MS.MTRL = I.MTRL AND NULLIF(ISNULL(MS.CODE,''),'') IS NOT NULL " +
    "FOR XML PATH(''), TYPE).value('.', 'varchar(max)'), 1, 1, ''), '')";
  var soTypeExpr = _bv_col_expr("I", "MTRL", ["SODTYPE", "SOTYPE"], "0");
  var activeExpr = _bv_col_expr("I", "MTRL", ["ISACTIVE"], "1");
  var brandExpr = _bv_col_expr("I", "MTRL", ["MTRMARK"], "NULL");
  var manufacturerExpr = _bv_col_expr("I", "MTRL", ["MTRMANFCTR"], "NULL");
  var hasMtrsup = _bv_has_column("MTRL", "MTRSUP");
  var preferredSupplierIdExpr = hasMtrsup ? "I.MTRSUP" : "NULL";
  var preferredSupplierNameSql = hasMtrsup ? "CAST(ISNULL(SUP.NAME, '') AS VARCHAR(255))" : "CAST('' AS VARCHAR(255))";
  var preferredSupplierJoinSql = hasMtrsup ? "LEFT JOIN TRDR SUP ON SUP.TRDR = I.MTRSUP AND SUP.COMPANY = I.COMPANY " : "";
  var cat1Expr = _bv_col_expr("PC1", "CCC88POCAT1", ["NAME"], "''");
  var cat2Expr = _bv_col_expr("PC2", "CCC88POCAT2", ["NAME"], "''");
  var cat3Expr = _bv_col_expr("PC3", "CCC88POCAT3", ["NAME"], "''");
  var groupExpr = _bv_col_expr("I", "MTRL", ["MTRGROUP"], "NULL");
  var commercialCategoryExpr = _bv_col_expr("I", "MTRL", ["MTRPCATEGORY"], "NULL");
  var vatExpr = _bv_col_expr("I", "MTRL", ["VAT"], "NULL");
  var manualOrderInfo = _bv_item_manual_order_info("I");
  var manualOrderCategoryExpr = manualOrderInfo.expr;
  var commercialStatusExpr = manualOrderInfo.commercialStatusExpr;
  var itemExtraJoinSql = manualOrderInfo.joinSql;
  var replInfo = _bv_item_replenishment_info("I");
  var itemReplJoinSql = replInfo.joinSql;
  var snapshotAsOf = cfg.toDate !== "" ? _bv_sql_quote(cfg.toDate) : "GETDATE()";

  var sql =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(ISNULL(" +
    codeExpr +
    ", I.MTRL) AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" +
    codeExpr +
    ", I.MTRL) AS VARCHAR(128)) AS ITEM_EXTERNAL_ID," +
    "CAST('ITEM|' + CAST(ISNULL(I.COMPANY,0) AS VARCHAR(32)) + '|' + CAST(ISNULL(I.MTRL,0) AS VARCHAR(40)) AS VARCHAR(128)) AS EXTERNAL_ID," +
    "CAST('ITEM|' + CAST(ISNULL(I.COMPANY,0) AS VARCHAR(32)) + '|' + CAST(ISNULL(I.MTRL,0) AS VARCHAR(40)) AS VARCHAR(128)) AS EVENT_ID," +
    "CONVERT(VARCHAR(10), CAST(" +
    snapshotAsOf +
    " AS DATE), 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" +
    updatedAtExpr +
    ", GETDATE()), 126) AS UPDATED_AT," +
    "CAST(ISNULL(" +
    nameExpr +
    ", '') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(NULLIF(ISNULL(" +
    barcodeExpr +
    ", ''), '') AS VARCHAR(128)) AS BARCODE," +
    "CAST(" +
    altBarcodeExpr +
    " AS VARCHAR(1024)) AS ALTERNATE_BARCODES," +
    "CAST(ISNULL(" +
    soTypeExpr +
    ", 0) AS INT) AS SOFTONE_SOTYPE," +
    "CAST(NULLIF(CAST(ISNULL(" +
    brandExpr +
    ", 0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS BRAND_EXTERNAL_ID," +
    "CAST(ISNULL(MK.NAME, '') AS VARCHAR(255)) AS BRAND_NAME," +
    "CAST(NULLIF(CAST(ISNULL(" +
    manufacturerExpr +
    ", 0) AS VARCHAR(128)), '0') AS VARCHAR(128)) AS MANUFACTURER_CODE," +
    "CAST(ISNULL(MF.NAME, '') AS VARCHAR(255)) AS MANUFACTURER_NAME," +
    "CAST(NULLIF(CAST(ISNULL(" +
    preferredSupplierIdExpr +
    ", 0) AS VARCHAR(128)), '0') AS VARCHAR(128)) AS PREFERRED_SUPPLIER_EXT_ID," +
    preferredSupplierNameSql +
    " AS PREFERRED_SUPPLIER_NAME," +
    "CAST(" + vatExpr + " AS DECIMAL(18,4)) AS VAT_RATE," +
    "CAST(ISNULL(VT.NAME, '') AS VARCHAR(255)) AS VAT_LABEL," +
    "CAST(ISNULL(" +
    cat1Expr +
    ", '') AS VARCHAR(255)) AS CATEGORY_1," +
    "CAST(ISNULL(" +
    cat2Expr +
    ", '') AS VARCHAR(255)) AS CATEGORY_2," +
    "CAST(ISNULL(" +
    cat3Expr +
    ", '') AS VARCHAR(255)) AS CATEGORY_3," +
    "CAST(NULLIF(CAST(ISNULL(" +
    groupExpr +
    ", 0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS GROUP_EXT_ID," +
    "CAST(NULLIF(CAST(ISNULL(" +
    groupExpr +
    ", 0) AS VARCHAR(64)), '0') AS VARCHAR(64)) AS GROUP_EXTERNAL_ID," +
    "CAST(ISNULL(MG.NAME, '') AS VARCHAR(255)) AS GROUP_NAME," +
    "CAST(ISNULL(CG.NAME, '') AS VARCHAR(255)) AS COMMERCIAL_CATEGORY," +
    "CAST(ISNULL(" +
    manualOrderCategoryExpr +
    ", '') AS VARCHAR(128)) AS MANUAL_ORDER_CATEGORY," +
    "CAST(ISNULL(" +
    commercialStatusExpr +
    ", '') AS VARCHAR(128)) AS COMMERCIAL_STATUS," +
    "CAST(ISNULL(" + replInfo.status1Expr + ", '') AS VARCHAR(64)) AS REPLENISHMENT_STATUS_1," +
    "CAST(ISNULL(" + replInfo.status2Expr + ", '') AS VARCHAR(128)) AS REPLENISHMENT_STATUS_2," +
    "CAST(" + replInfo.minStockExpr + " AS FLOAT) AS MIN_STOCK," +
    "CAST(" + replInfo.replMoqExpr + " AS FLOAT) AS REPLENISHMENT_MOQ," +
    "CAST(" + replInfo.vendorMoqExpr + " AS FLOAT) AS VENDOR_MOQ," +
    "CAST(" + replInfo.purchasePriceExpr + " AS FLOAT) AS CURRENT_PURCHASE_PRICE," +
    "CAST(ISNULL(" +
    activeExpr +
    ", 1) AS INT) AS IS_ACTIVE_SOURCE," +
    "CAST(ISNULL(I.COMPANY,0) AS VARCHAR(64)) AS COMPANY_ID " +
    "FROM MTRL I " +
    "LEFT JOIN MTRMARK MK ON MK.MTRMARK = I.MTRMARK AND MK.COMPANY = I.COMPANY " +
    "LEFT JOIN MTRMANFCTR MF ON MF.MTRMANFCTR = I.MTRMANFCTR AND MF.COMPANY = I.COMPANY " +
    preferredSupplierJoinSql +
    itemExtraJoinSql +
    itemReplJoinSql +
    "LEFT JOIN MTRGROUP MG ON MG.MTRGROUP = I.MTRGROUP AND MG.COMPANY = I.COMPANY " +
    "LEFT JOIN VAT VT ON VT.VAT = I.VAT " +
    "LEFT JOIN CCC88POCAT1 PC1 ON PC1.CCC88POCAT1 = I.CCC88POCAT1 " +
    "LEFT JOIN CCC88POCAT2 PC2 ON PC2.CCC88POCAT2 = I.CCC88POCAT2 " +
    "LEFT JOIN CCC88POCAT3 PC3 ON PC3.CCC88POCAT3 = I.CCC88POCAT3 " +
    "LEFT JOIN MTRPCATEGORY CG ON CG.MTRPCATEGORY = I.MTRPCATEGORY AND CG.COMPANY = I.COMPANY " +
    "WHERE I.COMPANY=" +
    cfg.company +
    " AND ISNULL(" +
    codeExpr +
    ", '') <> '' " +
    "ORDER BY " +
    codeExpr +
    " ASC";
  return sql;
}

function _bv_cash_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var branchInfo = _bv_branch_info_expr(c);
  var accountJoinSql = "";
  var accountIdExpr = "CAST(ISNULL(" + c.account + ",0) AS VARCHAR(128))";
  var hasTrdflines = _bv_table_exists("TRDFLINES");
  var cashSubcategoryExpr =
    "CASE " +
    "WHEN F.SOSOURCE=1381 THEN 'customer_collections' " +
    "WHEN F.SOSOURCE=1413 THEN 'customer_transfers' " +
    "WHEN F.SOSOURCE=1281 THEN 'supplier_payments' " +
    "WHEN F.SOSOURCE IN (1412,1416) THEN 'supplier_transfers' " +
    "WHEN F.SOSOURCE IN (1414,1481) THEN 'financial_accounts' " +
    "WHEN F.SOSOURCE=1261 THEN 'supplier_payments' " +
    "WHEN ISNULL(" + c.sodtype + ",0)=13 THEN 'customer_collections' " +
    "WHEN ISNULL(" + c.sodtype + ",0)=12 THEN 'supplier_payments' " +
    "WHEN ISNULL(" + c.sodtype + ",0)=14 THEN 'financial_accounts' " +
    "ELSE 'financial_accounts' END";
  var counterpartyTypeExpr =
    "CASE " +
    "WHEN ISNULL(" + c.sodtype + ",0)=13 THEN 'customer' " +
    "WHEN ISNULL(" + c.sodtype + ",0)=12 THEN 'supplier' " +
    "WHEN ISNULL(" + c.sodtype + ",0)=14 THEN 'internal' " +
    "ELSE 'internal' END";
  if (_bv_table_exists("SOCASH")) {
    // The cash register/account of a cash document is FINDOC.SOCASH -> the SOCASH
    // table (mirrors the SQL querypack). This is the real "Ταμείο" — ACNT/ACCOUNT is not.
    var scName = _bv_col_expr("SC", "SOCASH", ["NAME", "DESCR"], "NULL");
    var scCode = _bv_col_expr("SC", "SOCASH", ["CODE"], "NULL");
    var fSocash = _bv_col_expr("F", "FINDOC", ["SOCASH"], "0");
    accountJoinSql = " LEFT JOIN SOCASH SC ON SC.SOCASH=" + fSocash + " AND SC.COMPANY=F.COMPANY";
    accountIdExpr =
      "CAST(NULLIF(LTRIM(RTRIM(COALESCE(NULLIF(" + scName + ",''), NULLIF(" + scCode + ",''), CAST(NULLIF(" + fSocash + ",0) AS VARCHAR(128))))),'') AS VARCHAR(128))";
  } else if (_bv_table_exists("ACCOUNT")) {
    var accId = _bv_col_expr("AC", "ACCOUNT", ["ACCOUNT", "ACNT"], "0");
    var accCompany = _bv_col_expr("AC", "ACCOUNT", ["COMPANY"], c.company);
    var accCode = _bv_col_expr("AC", "ACCOUNT", ["CODE", "ACCCODE"], "''");
    accountJoinSql =
      " LEFT JOIN ACCOUNT AC ON " + accId + "=" + c.account + " AND " + accCompany + "=" + c.company;
    accountIdExpr = "CAST(ISNULL(" + accCode + ", " + c.account + ") AS VARCHAR(128))";
  }

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.cashSourceCodes + ")";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var headerSql =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" +
    c.finCode +
    " AS VARCHAR(128)) AS TRANSACTION_ID," +
    "CAST(" +
    c.finCode +
    " AS VARCHAR(128)) AS REFERENCE_NO," +
    "CONVERT(VARCHAR(10), " +
    c.trnDate +
    ", 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" +
    c.updDate +
    ", " +
    c.trnDate +
    "), 126) AS UPDATED_AT," +
    "CAST(ISNULL(" +
    c.branch +
    ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr +
    " AS BRANCH_NAME," +
    "CAST(ISNULL(" +
    c.company +
    ",0) AS VARCHAR(64)) AS COMPANY_ID," +
    accountIdExpr +
    " AS ACCOUNT_ID," +
    "CAST(ISNULL(T.CODE, " +
    c.trdr +
    ") AS VARCHAR(128)) AS COUNTERPARTY_ID," +
    "CAST(ISNULL(T.NAME, '') AS VARCHAR(255)) AS COUNTERPARTY_NAME," +
    "CAST(ABS(ISNULL(" +
    c.sumAmount +
    ",0)) AS FLOAT) AS AMOUNT_ABS," +
    "CAST(ISNULL(" +
    c.sumAmount +
    ",0) AS FLOAT) AS AMOUNT_RAW," +
    "CAST(ISNULL(" +
    c.comments +
    ",'') AS VARCHAR(1024)) AS NOTES," +
    "CASE WHEN ISNULL(F.TFPRMS,0)=101 AND ISNULL(" + c.sumAmount + ",0) < 0 THEN '102' WHEN ISNULL(F.TFPRMS,0) IN (2,102,181) THEN '102' WHEN ISNULL(F.TFPRMS,0)=101 THEN '101' WHEN ISNULL(" + c.sumAmount + ",0) < 0 THEN '102' ELSE '101' END AS TRANSACTION_TYPE," +
    cashSubcategoryExpr + " AS SUBCATEGORY," +
    counterpartyTypeExpr + " AS COUNTERPARTY_TYPE," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(ISNULL(" +
    c.soredir +
    ",0) AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(ISNULL(" +
    c.sodtype +
    ",0) AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(F.TFPRMS,0) AS INT) AS SOURCE_TRANSACTION_TYPE_ID," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS INT) AS OBJECT_ID " +
    "FROM FINDOC F " +
    "LEFT JOIN TRDR T ON T.TRDR=" +
    c.trdr +
    " AND T.COMPANY=F.COMPANY " +
    accountJoinSql +
    branchInfo.joinSql +
    whereSql;

  if (!hasTrdflines) {
    return headerSql + " ORDER BY " + c.trnDate + " ASC, " + c.findoc + " ASC";
  }

  var tlLineNo = _bv_col_expr("TL", "TRDFLINES", ["LINENUM", "TRDFLINES"], "0");
  var tlValue = _bv_col_expr("TL", "TRDFLINES", ["LINEVAL", "AMOUNT", "TRNVAL", "VAL"], "0");
  var lineWhereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (1261,1281,1381,1412,1413,1416)";
  if (cfg.fromDate !== "") lineWhereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") lineWhereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";
  var headerWithoutLinesWhereSql =
    whereSql +
    " AND (F.SOSOURCE NOT IN (1261,1281,1381,1412,1413,1416) OR NOT EXISTS (SELECT 1 FROM TRDFLINES TL WHERE TL.FINDOC=" +
    c.findoc +
    " AND TL.COMPANY=F.COMPANY))";

  var lineSql =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(" + c.findoc + " AS VARCHAR(40)) + '-' + CAST(ISNULL(" + tlLineNo + ",0) AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" + c.findoc + " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" + c.finCode + " AS VARCHAR(128)) AS TRANSACTION_ID," +
    "CAST(" + c.finCode + " AS VARCHAR(128)) AS REFERENCE_NO," +
    "CONVERT(VARCHAR(10), " + c.trnDate + ", 23) AS DOC_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" + c.updDate + ", " + c.trnDate + "), 126) AS UPDATED_AT," +
    "CAST(ISNULL(" + c.branch + ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr + " AS BRANCH_NAME," +
    "CAST(ISNULL(" + c.company + ",0) AS VARCHAR(64)) AS COMPANY_ID," +
    accountIdExpr + " AS ACCOUNT_ID," +
    "CAST(ISNULL(T.CODE, " + c.trdr + ") AS VARCHAR(128)) AS COUNTERPARTY_ID," +
    "CAST(ISNULL(T.NAME, '') AS VARCHAR(255)) AS COUNTERPARTY_NAME," +
    "CAST(ABS(ISNULL(" + tlValue + ",0)) AS FLOAT) AS AMOUNT_ABS," +
    "CAST(ISNULL(" + tlValue + ",0) AS FLOAT) AS AMOUNT_RAW," +
    "CAST(ISNULL(" + c.comments + ",'') AS VARCHAR(1024)) AS NOTES," +
    "CASE WHEN ISNULL(F.TFPRMS,0)=101 AND ISNULL(" + tlValue + ",0) < 0 THEN '102' WHEN ISNULL(F.TFPRMS,0) IN (2,102,181) THEN '102' WHEN ISNULL(F.TFPRMS,0)=101 THEN '101' WHEN ISNULL(" + tlValue + ",0) < 0 THEN '102' ELSE '101' END AS TRANSACTION_TYPE," +
    cashSubcategoryExpr + " AS SUBCATEGORY," +
    counterpartyTypeExpr + " AS COUNTERPARTY_TYPE," +
    "CAST(ISNULL(" + c.sosource + ",0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(ISNULL(" + c.soredir + ",0) AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(ISNULL(" + c.sodtype + ",0) AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(F.TFPRMS,0) AS INT) AS SOURCE_TRANSACTION_TYPE_ID," +
    "CAST(ISNULL(" + c.sosource + ",0) + ISNULL(" + c.soredir + ",0) AS INT) AS OBJECT_ID " +
    "FROM FINDOC F " +
    "INNER JOIN TRDFLINES TL ON TL.FINDOC=" + c.findoc + " AND TL.COMPANY=F.COMPANY " +
    "LEFT JOIN TRDR T ON T.TRDR=" + c.trdr + " AND T.COMPANY=F.COMPANY " +
    accountJoinSql +
    branchInfo.joinSql +
    lineWhereSql;

  headerSql = headerSql.split(whereSql).join(headerWithoutLinesWhereSql);

  return "SELECT * FROM (" + lineSql + " UNION ALL " + headerSql + ") C ORDER BY DOC_DATE ASC, DOCUMENT_ID ASC, EVENT_ID ASC";
}

function _bv_supplier_balances_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var branchInfo = _bv_branch_info_expr(c);
  var supplierAfm = _bv_col_expr("T", "TRDR", ["AFM"], "''");
  var asOfExpr = cfg.toDate !== "" ? _bv_sql_quote(cfg.toDate) : "CONVERT(VARCHAR(10), GETDATE(), 23)";
  var dueBaseExpr = "ISNULL(" + c.dueDate + ", " + c.trnDate + ")";
  var supplierBaseAmount = "ABS(ISNULL(" + c.sumAmount + ",0) + ISNULL(" + c.taxAmount + ",0))";
  var supplierBalanceSign =
    "(CASE " +
    "WHEN ISNULL(F.SOSOURCE,0) IN (1251,1261,1653) THEN " + supplierBaseAmount + " " +
    "WHEN ISNULL(F.SOSOURCE,0)=1253 THEN -" + supplierBaseAmount + " " +
    "WHEN ISNULL(F.SOSOURCE,0) IN (1281,1412,1416) THEN -" + supplierBaseAmount + " " +
    "ELSE (ISNULL(" + c.sumAmount + ",0) + ISNULL(" + c.taxAmount + ",0)) END)";

  var whereSql =
    " WHERE F.COMPANY=" +
    cfg.company +
    " AND F.SODTYPE=12 AND F.SOSOURCE IN (" +
    cfg.supplierBalanceSourceCodes +
    ")";
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT " +
    _bv_balance_top_clause(cfg) +
    "CAST(ISNULL(T.CODE, " +
    c.trdr +
    ") AS VARCHAR(64)) AS SUPPLIER_EXT_ID," +
    "CAST(ISNULL(T.NAME, '') AS VARCHAR(255)) AS SUPPLIER_NAME," +
    "CAST(ISNULL(" +
    supplierAfm +
    ", '') AS VARCHAR(64)) AS SUPPLIER_AFM," +
    "CAST(ISNULL(" +
    c.branch +
    ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    "CAST(MAX(" +
    branchInfo.branchNameExpr +
    ") AS VARCHAR(255)) AS BRANCH_NAME," +
    "CAST(ISNULL(MAX(" +
    c.company +
    "),0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CONVERT(VARCHAR(10), " +
    asOfExpr +
    ", 23) AS BALANCE_DATE," +
    "CONVERT(VARCHAR(10), " +
    asOfExpr +
    ", 23) AS DOC_DATE," +
    "CAST(SUM(" + supplierBalanceSign + ") AS FLOAT) AS OPEN_BALANCE," +
    "CAST(SUM(CASE " +
    "WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") > 0 AND " +
    supplierBalanceSign +
    " > 0 THEN " +
    supplierBalanceSign +
    "ELSE 0 END) AS FLOAT) AS OVERDUE_BALANCE," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") BETWEEN 0 AND 30 THEN " +
    "(CASE WHEN " + supplierBalanceSign + " > 0 THEN " + supplierBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_0_30," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") BETWEEN 31 AND 60 THEN " +
    "(CASE WHEN " + supplierBalanceSign + " > 0 THEN " + supplierBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_31_60," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") BETWEEN 61 AND 90 THEN " +
    "(CASE WHEN " + supplierBalanceSign + " > 0 THEN " + supplierBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_61_90," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") > 90 THEN " +
    "(CASE WHEN " + supplierBalanceSign + " > 0 THEN " + supplierBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_90_PLUS," +
    "CONVERT(VARCHAR(10), MAX(CASE WHEN F.SOSOURCE IN (1281,1412,1416) THEN " +
    c.trnDate +
    " ELSE NULL END), 23) AS LAST_PAYMENT_DATE," +
    "CAST(0 AS FLOAT) AS TREND_VS_PREVIOUS," +
    "CONVERT(VARCHAR(19), MAX(ISNULL(" +
    c.updDate +
    ", " +
    c.trnDate +
    ")), 126) AS UPDATED_AT " +
    "FROM FINDOC F " +
    "LEFT JOIN TRDR T ON T.TRDR=" +
    c.trdr +
    " AND T.COMPANY=F.COMPANY " +
    branchInfo.joinSql +
    whereSql +
    " GROUP BY ISNULL(T.CODE, " +
    c.trdr +
    "), ISNULL(T.NAME,''), ISNULL(" +
    supplierAfm +
    ",''), ISNULL(" +
    c.branch +
    ",0) " +
    " ORDER BY ISNULL(T.CODE, " +
    c.trdr +
    ")";

  return sql;
}

function _bv_customer_balances_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var branchInfo = _bv_branch_info_expr(c);
  var customerAfm = _bv_col_expr("T", "TRDR", ["AFM"], "''");
  var asOfExpr = cfg.toDate !== "" ? _bv_sql_quote(cfg.toDate) : "CONVERT(VARCHAR(10), GETDATE(), 23)";
  var dueBaseExpr = "ISNULL(" + c.dueDate + ", " + c.trnDate + ")";
  var customerBaseAmount = "ABS(ISNULL(" + c.sumAmount + ",0) + ISNULL(" + c.taxAmount + ",0))";
  var customerBalanceSign =
    "(CASE " +
    "WHEN ISNULL(F.SOSOURCE,0)=1351 AND ISNULL(F.SOREDIR,0) IN (0,10000) AND ISNULL(F.TFPRMS,0) IN (102,103,131) THEN " + customerBaseAmount + " " +
    "WHEN ISNULL(F.SOSOURCE,0)=1351 AND ISNULL(F.SOREDIR,0) IN (0,10000) AND ISNULL(F.TFPRMS,0) IN (151,152,181) THEN -" + customerBaseAmount + " " +
    "WHEN ISNULL(F.SOSOURCE,0)<>1351 AND ISNULL(F.TFPRMS,0) IN (101,131) THEN " + customerBaseAmount + " " +
    "WHEN ISNULL(F.SOSOURCE,0)<>1351 AND ISNULL(F.TFPRMS,0) IN (102,181) THEN -" + customerBaseAmount + " " +
    "WHEN ISNULL(F.SOSOURCE,0) IN (1381,1413) THEN -" + customerBaseAmount + " " +
    "ELSE (ISNULL(" + c.sumAmount + ",0) + ISNULL(" + c.taxAmount + ",0)) END)";

  var whereSql =
    " WHERE F.COMPANY=" +
    cfg.company +
    " AND F.SODTYPE=13 AND ((ISNULL(F.SOSOURCE,0)=1351 AND ISNULL(F.SOREDIR,0) IN (0,10000) AND ISNULL(F.TFPRMS,0) IN (102,103,131,151,152,181)) OR (ISNULL(F.SOSOURCE,0)<>1351 AND ISNULL(F.TFPRMS,0) IN (101,102,131,181)) OR ISNULL(F.SOSOURCE,0) IN (1381,1413))";
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT " +
    _bv_balance_top_clause(cfg) +
    "CAST(ISNULL(T.CODE, " +
    c.trdr +
    ") AS VARCHAR(64)) AS CUSTOMER_EXT_ID," +
    "CAST(ISNULL(T.NAME, '') AS VARCHAR(255)) AS CUSTOMER_NAME," +
    "CAST(ISNULL(" +
    customerAfm +
    ", '') AS VARCHAR(64)) AS CUSTOMER_AFM," +
    "CAST(ISNULL(" +
    c.branch +
    ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    "CAST(MAX(" +
    branchInfo.branchNameExpr +
    ") AS VARCHAR(255)) AS BRANCH_NAME," +
    "CAST(ISNULL(MAX(" +
    c.company +
    "),0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CONVERT(VARCHAR(10), " +
    asOfExpr +
    ", 23) AS BALANCE_DATE," +
    "CONVERT(VARCHAR(10), " +
    asOfExpr +
    ", 23) AS DOC_DATE," +
    "CAST(SUM(" + customerBalanceSign + ") AS FLOAT) AS OPEN_BALANCE," +
    "CAST(SUM(CASE " +
    "WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") > 0 AND " +
    customerBalanceSign +
    " > 0 THEN " +
    customerBalanceSign +
    "ELSE 0 END) AS FLOAT) AS OVERDUE_BALANCE," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") BETWEEN 0 AND 30 THEN " +
    "(CASE WHEN " + customerBalanceSign + " > 0 THEN " + customerBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_0_30," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") BETWEEN 31 AND 60 THEN " +
    "(CASE WHEN " + customerBalanceSign + " > 0 THEN " + customerBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_31_60," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") BETWEEN 61 AND 90 THEN " +
    "(CASE WHEN " + customerBalanceSign + " > 0 THEN " + customerBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_61_90," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    dueBaseExpr +
    ", " + asOfExpr + ") > 90 THEN " +
    "(CASE WHEN " + customerBalanceSign + " > 0 THEN " + customerBalanceSign + " ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_90_PLUS," +
    "CONVERT(VARCHAR(10), MAX(CASE WHEN F.SOSOURCE IN (1381,1413) THEN " +
    c.trnDate +
    " ELSE NULL END), 23) AS LAST_COLLECTION_DATE," +
    "CAST(0 AS FLOAT) AS TREND_VS_PREVIOUS," +
    "CONVERT(VARCHAR(19), MAX(ISNULL(" +
    c.updDate +
    ", " +
    c.trnDate +
    ")), 126) AS UPDATED_AT " +
    "FROM FINDOC F " +
    "LEFT JOIN TRDR T ON T.TRDR=" +
    c.trdr +
    " AND T.COMPANY=F.COMPANY " +
    branchInfo.joinSql +
    whereSql +
    " GROUP BY ISNULL(T.CODE, " +
    c.trdr +
    "), ISNULL(T.NAME,''), ISNULL(" +
    customerAfm +
    ",''), ISNULL(" +
    c.branch +
    ",0) " +
    " ORDER BY ISNULL(T.CODE, " +
    c.trdr +
    ")";

  return sql;
}

function _bv_expenses_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var seriesInfo = _bv_series_info_expr(c);
  var branchInfo = _bv_branch_info_expr(c);
  var lLineId = _bv_col_expr("L", "MTRLINES", ["MTRLINES", "LINENUM"], "0");
  var lMtrl = _bv_col_expr("L", "MTRLINES", ["MTRL"], "0");
  var iName = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var lNet = _bv_col_expr("L", "MTRLINES", ["NETLINEVAL", "NETVAL", "NETAMNT", "LINEVAL"], "0");
  var lVat = _bv_col_expr("L", "MTRLINES", ["VATAMNT", "TAXAMNT", "FPAAMNT", "LINEVAT", "LINETAX", "LINEVATAMNT", "LINETAXAMNT"], "0");
  var expenseSign = "(CASE WHEN ISNULL(F.TFPRMS,0) IN (151,152) THEN -1 ELSE 1 END)";
  var specialExpenseSign = "(CASE WHEN ISNULL(F.TFPRMS,0)=102 THEN -1 ELSE 1 END)";
  var expenseLikeSeriesExpr =
    "((ISNULL(" + c.sosource + ",0)=1253 AND ISNULL(F.TFPRMS,0)=101)" +
    " OR ISNULL(" + c.series + ",0) IN (1001,1002,1003,1006,1007,1009,1102,3201)" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%Δαπαν%'" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%παροχής υπηρεσι%'" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%Αγοράς Παγίων%'" +
    " OR ISNULL(" + seriesInfo.seriesNameExpr + ",'') LIKE N'%Λογαριασμ%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Δαπάν%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Έξοδ%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Πάγι%'" +
    " OR ISNULL(" + iName + ",'') LIKE N'%Υπηρεσ%')";

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.expenseSourceCodes + ")";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql1261 =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" +
    c.findoc +
    " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" +
    c.finCode +
    " AS VARCHAR(128)) AS DOCUMENT_NO," +
    "CONVERT(VARCHAR(10), " +
    c.trnDate +
    ", 23) AS EXPENSE_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" +
    c.updDate +
    ", " +
    c.trnDate +
    "), 126) AS UPDATED_AT," +
    "CAST(ISNULL(" +
    c.branch +
    ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr +
    " AS BRANCH_NAME," +
    "CAST(ISNULL(" +
    c.company +
    ",0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CAST(ISNULL(S.CODE, " +
    c.trdr +
    ") AS VARCHAR(64)) AS SUPPLIER_EXT_ID," +
    "CAST(ISNULL(S.NAME, '') AS VARCHAR(255)) AS SUPPLIER_NAME," +
    "CAST(ISNULL(" +
    c.account +
    ",0) AS VARCHAR(128)) AS ACCOUNT_ID," +
    "CAST('operational' AS VARCHAR(128)) AS EXPENSE_CATEGORY_CODE," +
    "CAST(ISNULL(" + seriesInfo.seriesNameExpr + ",'Λειτουργικά Έξοδα') AS VARCHAR(255)) AS EXPENSE_CATEGORY_NAME," +
    "CAST(CAST(ISNULL(F.TFPRMS,0) AS VARCHAR(16)) + ' ' + ISNULL(" +
    seriesInfo.seriesNameExpr +
    ",'expense_' + CAST(ISNULL(" +
    c.sosource +
    ",0) AS VARCHAR(16))) AS VARCHAR(128)) AS DOCUMENT_TYPE," +
    "CAST(" + specialExpenseSign + " * ABS(ISNULL(" +
    c.sumAmount +
    ",0)) AS FLOAT) AS AMOUNT_NET," +
    "CAST(" + specialExpenseSign + " * ABS(ISNULL(" +
    c.taxAmount +
    ",0)) AS FLOAT) AS AMOUNT_TAX," +
    "CAST(" + specialExpenseSign + " * ABS(ISNULL(" +
    c.sumAmount +
    ",0) + ISNULL(" +
    c.taxAmount +
    ",0)) AS FLOAT) AS AMOUNT_GROSS," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(ISNULL(" +
    c.soredir +
    ",0) AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(ISNULL(" +
    c.sodtype +
    ",0) AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS INT) AS OBJECT_ID " +
    "FROM FINDOC F " +
    "LEFT JOIN TRDR S ON S.TRDR=" +
    c.trdr +
    " AND S.COMPANY=F.COMPANY " +
    branchInfo.joinSql +
    seriesInfo.joinSql +
    whereSql;

  var purchaseExpenseWhereSql =
    " WHERE F.COMPANY=" + cfg.company +
    " AND ISNULL(" + c.sodtype + ",0)=12" +
    " AND F.SOSOURCE IN (" + cfg.purchaseSourceCodes + ")" +
    " AND " + expenseLikeSeriesExpr;
  if (cfg.fromDate !== "") purchaseExpenseWhereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") purchaseExpenseWhereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sqlPurchaseExpenses =
    "SELECT " +
    _bv_top_clause(cfg.limit) +
    "CAST(" + c.findoc + " AS VARCHAR(40)) + '-' + CAST(ISNULL(" + lLineId + ",0) AS VARCHAR(40)) AS EVENT_ID," +
    "CAST(" + c.findoc + " AS VARCHAR(40)) AS DOCUMENT_ID," +
    "CAST(" + c.finCode + " AS VARCHAR(128)) AS DOCUMENT_NO," +
    "CONVERT(VARCHAR(10), " + c.trnDate + ", 23) AS EXPENSE_DATE," +
    "CONVERT(VARCHAR(19), ISNULL(" + c.updDate + ", " + c.trnDate + "), 126) AS UPDATED_AT," +
    "CAST(ISNULL(" + c.branch + ",0) AS VARCHAR(64)) AS BRANCH_EXT_ID," +
    branchInfo.branchNameExpr +
    " AS BRANCH_NAME," +
    "CAST(ISNULL(" + c.company + ",0) AS VARCHAR(64)) AS COMPANY_ID," +
    "CAST(ISNULL(S.CODE, " + c.trdr + ") AS VARCHAR(64)) AS SUPPLIER_EXT_ID," +
    "CAST(ISNULL(S.NAME, '') AS VARCHAR(255)) AS SUPPLIER_NAME," +
    "CAST(NULL AS VARCHAR(128)) AS ACCOUNT_ID," +
    "CAST('softone_series_' + CAST(ISNULL(" + c.series + ",0) AS VARCHAR(32)) AS VARCHAR(128)) AS EXPENSE_CATEGORY_CODE," +
    "CAST(ISNULL(" + seriesInfo.seriesNameExpr + ",'Λοιπά Έξοδα / Δαπάνες') AS VARCHAR(255)) AS EXPENSE_CATEGORY_NAME," +
    "CAST(CAST(ISNULL(F.TFPRMS,0) AS VARCHAR(16)) + ' purchase_expense_' + CAST(ISNULL(" + c.sosource + ",0) + ISNULL(" + c.soredir + ",0) AS VARCHAR(16)) AS VARCHAR(128)) AS DOCUMENT_TYPE," +
    "CAST(" + expenseSign + " * ABS(ISNULL(" + lNet + ",0)) AS FLOAT) AS AMOUNT_NET," +
    "CAST(" + expenseSign + " * ABS(ISNULL(" + lVat + ",0)) AS FLOAT) AS AMOUNT_TAX," +
    "CAST(" + expenseSign + " * (ABS(ISNULL(" + lNet + ",0)) + ABS(ISNULL(" + lVat + ",0))) AS FLOAT) AS AMOUNT_GROSS," +
    "CAST(ISNULL(" + c.sosource + ",0) AS INT) AS SOURCE_MODULE_ID," +
    "CAST(ISNULL(" + c.soredir + ",0) AS INT) AS REDIRECT_MODULE_ID," +
    "CAST(ISNULL(" + c.sodtype + ",0) AS INT) AS SOURCE_ENTITY_ID," +
    "CAST(ISNULL(" + c.sosource + ",0) + ISNULL(" + c.soredir + ",0) AS INT) AS OBJECT_ID " +
    "FROM FINDOC F " +
    "INNER JOIN MTRLINES L ON L.FINDOC=" + c.findoc + " AND L.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRL I ON I.MTRL=" + lMtrl + " AND I.COMPANY=F.COMPANY " +
    "LEFT JOIN TRDR S ON S.TRDR=" + c.trdr + " AND S.COMPANY=F.COMPANY " +
    branchInfo.joinSql +
    seriesInfo.joinSql +
    purchaseExpenseWhereSql;

  return "SELECT * FROM (" + sql1261 + " UNION ALL " + sqlPurchaseExpenses + ") Z ORDER BY EXPENSE_DATE ASC, DOCUMENT_NO ASC";
}

function _bv_norm_for_match(v) {
  return _bv_text(v, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function _bv_inventory_is_financial(documentType, seriesName) {
  var txt = _bv_norm_for_match(documentType + " " + seriesName);
  var hasInvoice =
    txt.indexOf("τιμολ") >= 0 ||
    txt.indexOf("invoice") >= 0 ||
    txt.indexOf("τιμολογ") >= 0;
  var hasDelivery = txt.indexOf("δελτι") >= 0 || txt.indexOf("delivery") >= 0 || txt.indexOf("dispatch") >= 0;

  if (hasInvoice && hasDelivery) return true;
  if (hasDelivery && !hasInvoice) return false;
  return null;
}

function _bv_attach_common_doc_fields(rec, ds) {
  rec.source_module_id = _bv_int(_bv_field(ds, "SOURCE_MODULE_ID", 0), 0, 0, 99999999);
  rec.redirect_module_id = _bv_int(_bv_field(ds, "REDIRECT_MODULE_ID", 0), 0, 0, 99999999);
  rec.source_entity_id = _bv_int(_bv_field(ds, "SOURCE_ENTITY_ID", 0), 0, 0, 99999999);
  rec.object_id = _bv_int(_bv_field(ds, "OBJECT_ID", 0), rec.source_module_id + rec.redirect_module_id, 0, 99999999);
  return rec;
}

function _bv_attach_org_fields(rec, ds) {
  var companyId = _bv_text(_bv_field(ds, "COMPANY_ID", ""), "");
  var branchCode = _bv_text(rec.branch_ext_id, "");
  var branchName = _bv_text(_bv_field(ds, "BRANCH_NAME", branchCode), branchCode);

  if (companyId !== "" && branchCode !== "" && branchCode.indexOf(companyId + ":") !== 0) {
    rec.branch_ext_id = companyId + ":" + branchCode;
  } else {
    rec.branch_ext_id = branchCode;
  }

  if (typeof rec.branch_id !== "undefined") rec.branch_id = rec.branch_ext_id;
  rec.branch_code = branchCode;
  rec.company_id = companyId;
  rec.branch_name = branchName;
  return rec;
}

function _bv_sales_record(ds) {
  var eventId = _bv_text(_bv_field(ds, "EVENT_ID", ""), "");
  var rec = {
    event_id: eventId,
    external_id: eventId,
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", ""), ""),
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), ""),
    document_type: _bv_text(_bv_field(ds, "DOCUMENT_TYPE", "sales"), "sales"),
    document_status: _bv_text(_bv_field(ds, "DOCUMENT_STATUS", ""), ""),
    document_id: _bv_text(_bv_field(ds, "DOCUMENT_ID", ""), ""),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    document_series: _bv_text(_bv_field(ds, "DOCUMENT_SERIES", ""), ""),
    document_series_name: _bv_text(_bv_field(ds, "DOCUMENT_SERIES_NAME", ""), ""),
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    warehouse_ext_id: _bv_text(_bv_field(ds, "WAREHOUSE_EXT_ID", ""), ""),
    warehouse_name: _bv_text(_bv_field(ds, "WAREHOUSE_NAME", ""), ""),
    source_created_at: _bv_text(_bv_field(ds, "SOURCE_CREATED_AT", ""), ""),
    customer_ext_id: _bv_text(_bv_field(ds, "CUSTOMER_EXT_ID", ""), ""),
    customer_name: _bv_text(_bv_field(ds, "CUSTOMER_NAME", ""), ""),
    customer_afm: _bv_text(_bv_field(ds, "CUSTOMER_AFM", ""), ""),
    entity_ext_id: _bv_text(_bv_field(ds, "CUSTOMER_EXT_ID", ""), ""),
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    brand_external_id: _bv_text(_bv_field(ds, "BRAND_EXTERNAL_ID", ""), ""),
    brand_name: _bv_text(_bv_field(ds, "BRAND_NAME", ""), ""),
    group_ext_id: _bv_text(_bv_field(ds, "GROUP_EXT_ID", ""), ""),
    group_name: _bv_text(_bv_field(ds, "GROUP_NAME", ""), ""),
    supplier_external_id: _bv_text(_bv_field(ds, "SUPPLIER_EXTERNAL_ID", ""), ""),
    category_external_id: _bv_text(_bv_field(ds, "CATEGORY_EXTERNAL_ID", ""), ""),
    manual_order_category: _bv_text(_bv_field(ds, "MANUAL_ORDER_CATEGORY", ""), ""),
    commercial_status: _bv_text(_bv_field(ds, "COMMERCIAL_STATUS", ""), ""),
    discount_pct: _bv_num(_bv_field(ds, "DISCOUNT_PCT", 0), 0),
    discount_amount: _bv_num(_bv_field(ds, "DISCOUNT_AMOUNT", 0), 0),
    channel_ext_id: _bv_text(_bv_field(ds, "CHANNEL_EXT_ID", ""), ""),
    channel_name: _bv_text(_bv_field(ds, "CHANNEL_NAME", ""), ""),
    eshop_code: _bv_text(_bv_field(ds, "ESHOP_CODE", ""), ""),
    payment_method: _bv_text(_bv_field(ds, "PAYMENT_METHOD", ""), ""),
    shipping_method: _bv_text(_bv_field(ds, "SHIPPING_METHOD", ""), ""),
    reason: _bv_text(_bv_field(ds, "REASON", ""), ""),
    origin_ref: _bv_text(_bv_field(ds, "ORIGIN_REF", ""), ""),
    destination_ref: _bv_text(_bv_field(ds, "DESTINATION_REF", ""), ""),
    shipping_expense_value: _bv_num(_bv_field(ds, "SHIPPING_EXPENSE_VALUE", 0), 0),
    shipping_expense_description: _bv_text(_bv_field(ds, "SHIPPING_EXPENSE_DESCRIPTION", ""), ""),
    charge_revenue_net_value: _bv_num(_bv_field(ds, "CHARGE_REVENUE_NET_VALUE", 0), 0),
    charge_revenue_vat_value: _bv_num(_bv_field(ds, "CHARGE_REVENUE_VAT_VALUE", 0), 0),
    charge_revenue_gross_value: _bv_num(_bv_field(ds, "CHARGE_REVENUE_GROSS_VALUE", 0), 0),
    charge_revenue_description: _bv_text(_bv_field(ds, "CHARGE_REVENUE_DESCRIPTION", ""), ""),
    charge_revenue_lines_json: _bv_text(_bv_field(ds, "CHARGE_REVENUE_LINES_JSON", "[]"), "[]"),
    shipping_charge_net_value: _bv_num(_bv_field(ds, "SHIPPING_CHARGE_NET_VALUE", 0), 0),
    shipping_charge_vat_value: _bv_num(_bv_field(ds, "SHIPPING_CHARGE_VAT_VALUE", 0), 0),
    shipping_charge_gross_value: _bv_num(_bv_field(ds, "SHIPPING_CHARGE_GROSS_VALUE", 0), 0),
    cod_charge_net_value: _bv_num(_bv_field(ds, "COD_CHARGE_NET_VALUE", 0), 0),
    cod_charge_vat_value: _bv_num(_bv_field(ds, "COD_CHARGE_VAT_VALUE", 0), 0),
    cod_charge_gross_value: _bv_num(_bv_field(ds, "COD_CHARGE_GROSS_VALUE", 0), 0),
    gift_charge_net_value: _bv_num(_bv_field(ds, "GIFT_CHARGE_NET_VALUE", 0), 0),
    gift_charge_vat_value: _bv_num(_bv_field(ds, "GIFT_CHARGE_VAT_VALUE", 0), 0),
    gift_charge_gross_value: _bv_num(_bv_field(ds, "GIFT_CHARGE_GROSS_VALUE", 0), 0),
    other_charge_net_value: _bv_num(_bv_field(ds, "OTHER_CHARGE_NET_VALUE", 0), 0),
    other_charge_vat_value: _bv_num(_bv_field(ds, "OTHER_CHARGE_VAT_VALUE", 0), 0),
    other_charge_gross_value: _bv_num(_bv_field(ds, "OTHER_CHARGE_GROSS_VALUE", 0), 0),
    customer_branch: _bv_text(_bv_field(ds, "CUSTOMER_BRANCH", ""), ""),
    delivery_address: _bv_text(_bv_field(ds, "DELIVERY_ADDRESS", ""), ""),
    delivery_zip: _bv_text(_bv_field(ds, "DELIVERY_ZIP", ""), ""),
    delivery_city: _bv_text(_bv_field(ds, "DELIVERY_CITY", ""), ""),
    delivery_area: _bv_text(_bv_field(ds, "DELIVERY_AREA", ""), ""),
    movement_type: _bv_text(_bv_field(ds, "MOVEMENT_TYPE", ""), ""),
    carrier_name: _bv_text(_bv_field(ds, "CARRIER_NAME", ""), ""),
    transport_medium: _bv_text(_bv_field(ds, "TRANSPORT_MEDIUM", ""), ""),
    transport_no: _bv_text(_bv_field(ds, "TRANSPORT_NO", ""), ""),
    route_name: _bv_text(_bv_field(ds, "ROUTE_NAME", ""), ""),
    loading_date: _bv_text(_bv_field(ds, "LOADING_DATE", ""), ""),
    delivery_date: _bv_text(_bv_field(ds, "DELIVERY_DATE", ""), ""),
    voucher_no: _bv_text(_bv_field(ds, "VOUCHER_NO", ""), ""),
    voucher_url: _bv_text(_bv_field(ds, "VOUCHER_URL", ""), ""),
    locker_id: _bv_text(_bv_field(ds, "LOCKER_ID", ""), ""),
    order_comments: _bv_text(_bv_field(ds, "ORDER_COMMENTS", ""), ""),
    shipping_comments: _bv_text(_bv_field(ds, "SHIPPING_COMMENTS", ""), ""),
    gift_comments: _bv_text(_bv_field(ds, "GIFT_COMMENTS", ""), ""),
    marketplace_courier: _bv_text(_bv_field(ds, "MARKETPLACE_COURIER", ""), ""),
    marketplace_internal_id: _bv_text(_bv_field(ds, "MARKETPLACE_INTERNAL_ID", ""), ""),
    source_transaction_type_id: _bv_int(_bv_field(ds, "SOURCE_TRANSACTION_TYPE_ID", 0), 0, 0, 99999999),
    qty: _bv_num(_bv_field(ds, "QTY", 0), 0),
    net_value: _bv_num(_bv_field(ds, "NET_VALUE", 0), 0),
    line_value: _bv_num(_bv_field(ds, "LINE_VALUE", _bv_field(ds, "NET_VALUE", 0)), 0),
    gross_value: _bv_num(_bv_field(ds, "GROSS_VALUE", 0), 0),
    vat_amount: _bv_num(_bv_field(ds, "VAT_AMOUNT", 0), 0),
    doc_net_total: _bv_num(_bv_field(ds, "DOC_NET_TOTAL", 0), 0),
    doc_expenses_total: _bv_num(_bv_field(ds, "DOC_EXPENSES_TOTAL", 0), 0),
    doc_tax_total: _bv_num(_bv_field(ds, "DOC_TAX_TOTAL", 0), 0),
    doc_gross_total: _bv_num(_bv_field(ds, "DOC_GROSS_TOTAL", 0), 0),
    cost_amount: _bv_num(_bv_field(ds, "COST_AMOUNT", 0), 0),
    source_payload_json: ds
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_purchase_record(ds) {
  var eventId = _bv_text(_bv_field(ds, "EVENT_ID", ""), "");
  var rec = {
    event_id: eventId,
    external_id: eventId,
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", ""), ""),
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), ""),
    source_created_at: _bv_text(_bv_field(ds, "SOURCE_CREATED_AT", ""), ""),
    document_type: _bv_text(_bv_field(ds, "DOCUMENT_TYPE", "purchase"), "purchase"),
    document_id: _bv_text(_bv_field(ds, "DOCUMENT_ID", ""), ""),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    document_series: _bv_text(_bv_field(ds, "DOCUMENT_SERIES", ""), ""),
    document_series_name: _bv_text(_bv_field(ds, "DOCUMENT_SERIES_NAME", ""), ""),
    document_behavior_code: _bv_num(_bv_field(ds, "DOCUMENT_BEHAVIOR_CODE", 0), 0),
    purchase_flow: _bv_text(_bv_field(ds, "PURCHASE_FLOW", "purchase"), "purchase"),
    is_credit_or_return: _bv_bool(_bv_field(ds, "IS_CREDIT_OR_RETURN", 0), false),
    due_date: _bv_text(_bv_field(ds, "DUE_DATE", ""), ""),
    payment_terms_days: _bv_num(_bv_field(ds, "PAYMENT_TERMS_DAYS", 0), 0),
    payment_method: _bv_text(_bv_field(ds, "PAYMENT_METHOD", ""), ""),
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    warehouse_ext_id: _bv_text(_bv_field(ds, "WAREHOUSE_EXT_ID", ""), ""),
    warehouse_name: _bv_text(_bv_field(ds, "WAREHOUSE_NAME", ""), ""),
    supplier_ext_id: _bv_text(_bv_field(ds, "SUPPLIER_EXT_ID", ""), ""),
    supplier_name: _bv_text(_bv_field(ds, "SUPPLIER_NAME", ""), ""),
    supplier_afm: _bv_text(_bv_field(ds, "SUPPLIER_AFM", ""), ""),
    entity_ext_id: _bv_text(_bv_field(ds, "SUPPLIER_EXT_ID", ""), ""),
    channel_ext_id: _bv_text(_bv_field(ds, "CHANNEL_EXT_ID", ""), ""),
    channel_name: _bv_text(_bv_field(ds, "CHANNEL_NAME", ""), ""),
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    brand_external_id: _bv_text(_bv_field(ds, "BRAND_EXTERNAL_ID", ""), ""),
    brand_name: _bv_text(_bv_field(ds, "BRAND_NAME", ""), ""),
    group_external_id: _bv_text(_bv_field(ds, "GROUP_EXTERNAL_ID", ""), ""),
    group_name: _bv_text(_bv_field(ds, "GROUP_NAME", ""), ""),
    category_external_id: _bv_text(_bv_field(ds, "CATEGORY_EXTERNAL_ID", ""), ""),
    category_name: _bv_text(_bv_field(ds, "CATEGORY_NAME", ""), ""),
    manual_order_category: _bv_text(_bv_field(ds, "MANUAL_ORDER_CATEGORY", ""), ""),
    commercial_status: _bv_text(_bv_field(ds, "COMMERCIAL_STATUS", ""), ""),
    qty: _bv_num(_bv_field(ds, "QTY", 0), 0),
    net_value: _bv_num(_bv_field(ds, "NET_VALUE", 0), 0),
    line_value: _bv_num(_bv_field(ds, "LINE_VALUE", _bv_field(ds, "NET_VALUE", 0)), 0),
    gross_value: _bv_num(_bv_field(ds, "GROSS_VALUE", 0), 0),
    vat_amount: _bv_num(_bv_field(ds, "VAT_AMOUNT", 0), 0),
    doc_net_total: _bv_num(_bv_field(ds, "DOC_NET_TOTAL", 0), 0),
    doc_expenses_total: _bv_num(_bv_field(ds, "DOC_EXPENSES_TOTAL", 0), 0),
    doc_tax_total: _bv_num(_bv_field(ds, "DOC_TAX_TOTAL", 0), 0),
    doc_gross_total: _bv_num(_bv_field(ds, "DOC_GROSS_TOTAL", 0), 0),
    no_discount_amount: _bv_num(_bv_field(ds, "NO_DISCOUNT_AMOUNT", _bv_field(ds, "NODSCAMNT", 0)), 0),
    nodscamnt: _bv_num(_bv_field(ds, "NODSCAMNT", _bv_field(ds, "NO_DISCOUNT_AMOUNT", 0)), 0),
    discount_amount: _bv_num(_bv_field(ds, "DISCOUNT_AMOUNT", 0), 0),
    discount_pct: _bv_num(_bv_field(ds, "DISCOUNT_PCT", null), null),
    discount1_pct: _bv_num(_bv_field(ds, "DISCOUNT1_PCT", null), null),
    discount2_pct: _bv_num(_bv_field(ds, "DISCOUNT2_PCT", null), null),
    discount3_pct: _bv_num(_bv_field(ds, "DISCOUNT3_PCT", null), null),
    discount1_amount: _bv_num(_bv_field(ds, "DISCOUNT1_AMOUNT", null), null),
    discount2_amount: _bv_num(_bv_field(ds, "DISCOUNT2_AMOUNT", null), null),
    discount3_amount: _bv_num(_bv_field(ds, "DISCOUNT3_AMOUNT", null), null),
    cost_amount: _bv_num(_bv_field(ds, "COST_AMOUNT", 0), 0)
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_supplier_order_record(ds) {
  var eventId = _bv_text(_bv_field(ds, "EVENT_ID", ""), "");
  var rec = {
    event_id: eventId,
    external_id: eventId,
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", ""), ""),
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), ""),
    document_type: "supplier_order",
    document_id: _bv_text(_bv_field(ds, "DOCUMENT_ID", ""), ""),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    document_series: _bv_text(_bv_field(ds, "DOCUMENT_SERIES", ""), ""),
    document_series_name: _bv_text(_bv_field(ds, "DOCUMENT_SERIES_NAME", ""), ""),
    document_behavior_code: _bv_num(_bv_field(ds, "DOCUMENT_BEHAVIOR_CODE", 201), 201),
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    branch_name: _bv_text(_bv_field(ds, "BRANCH_NAME", ""), ""),
    supplier_ext_id: _bv_text(_bv_field(ds, "SUPPLIER_EXT_ID", ""), ""),
    supplier_name: _bv_text(_bv_field(ds, "SUPPLIER_NAME", ""), ""),
    supplier_afm: _bv_text(_bv_field(ds, "SUPPLIER_AFM", ""), ""),
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    order_qty: _bv_num(_bv_field(ds, "ORDER_QTY", 0), 0),
    covered_qty: _bv_num(_bv_field(ds, "COVERED_QTY", 0), 0),
    cancelled_qty: _bv_num(_bv_field(ds, "CANCELLED_QTY", 0), 0),
    line_value: _bv_num(_bv_field(ds, "LINE_VALUE", 0), 0),
    has_transformation: _bv_bool(_bv_field(ds, "HAS_TRANSFORMATION", 0), false),
    order_status: _bv_text(_bv_field(ds, "ORDER_STATUS", "open"), "open")
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_inventory_record(ds) {
  var eventId = _bv_text(_bv_field(ds, "EVENT_ID", ""), "");
  var documentType = _bv_text(_bv_field(ds, "DOCUMENT_TYPE", "inventory"), "inventory");
  var seriesName = _bv_text(_bv_field(ds, "DOCUMENT_SERIES_NAME", ""), "");
  var valueAmount = _bv_num(_bv_field(ds, "VALUE_AMOUNT", 0), 0);
  var retailValueAmount = _bv_num(_bv_field(ds, "RETAIL_VALUE_AMOUNT", valueAmount), valueAmount);
  var costAmount = _bv_num(_bv_field(ds, "COST_AMOUNT", 0), 0);
  var financialValueAmount = _bv_num(_bv_field(ds, "FINANCIAL_VALUE_AMOUNT", costAmount), costAmount);
  var financialFlag = _bv_inventory_is_financial(documentType, seriesName);

  var rec = {
    event_id: eventId,
    external_id: eventId,
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", ""), ""),
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), ""),
    document_type: documentType,
    document_id: _bv_text(_bv_field(ds, "DOCUMENT_ID", ""), ""),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    document_series: _bv_text(_bv_field(ds, "DOCUMENT_SERIES", ""), ""),
    document_series_name: seriesName,
    movement_type: _bv_text(_bv_field(ds, "MOVEMENT_TYPE", "entry"), "entry"),
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    warehouse_ext_id: _bv_text(_bv_field(ds, "WAREHOUSE_EXT_ID", ""), ""),
    warehouse_name: _bv_text(_bv_field(ds, "WAREHOUSE_NAME", ""), ""),
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    softone_sotype: _bv_int(_bv_field(ds, "SOFTONE_SOTYPE", 0), 0, 0, 99999999),
    barcode: _bv_text(_bv_field(ds, "BARCODE", ""), ""),
    alternate_barcodes: _bv_text(_bv_field(ds, "ALTERNATE_BARCODES", ""), ""),
    brand_external_id: _bv_text(_bv_field(ds, "BRAND_EXTERNAL_ID", ""), ""),
    brand_name: _bv_text(_bv_field(ds, "BRAND_NAME", ""), ""),
    group_external_id: _bv_text(_bv_field(ds, "GROUP_EXTERNAL_ID", ""), ""),
    group_name: _bv_text(_bv_field(ds, "GROUP_NAME", ""), ""),
    commercial_category: _bv_text(_bv_field(ds, "COMMERCIAL_CATEGORY", ""), ""),
    manufacturer_code: _bv_text(_bv_field(ds, "MANUFACTURER_CODE", ""), ""),
    manufacturer_name: _bv_text(_bv_field(ds, "MANUFACTURER_NAME", ""), ""),
    vat_rate: _bv_num(_bv_field(ds, "VAT_RATE", null), null),
    vat_label: _bv_text(_bv_field(ds, "VAT_LABEL", ""), ""),
    category_1: _bv_text(_bv_field(ds, "CATEGORY_1", ""), ""),
    category_2: _bv_text(_bv_field(ds, "CATEGORY_2", ""), ""),
    category_3: _bv_text(_bv_field(ds, "CATEGORY_3", ""), ""),
    manual_order_category: _bv_text(_bv_field(ds, "MANUAL_ORDER_CATEGORY", ""), ""),
    commercial_status: _bv_text(_bv_field(ds, "COMMERCIAL_STATUS", ""), ""),
    is_active: _bv_bool(_bv_field(ds, "IS_ACTIVE_SOURCE", 1), true),
    is_active_source: _bv_bool(_bv_field(ds, "IS_ACTIVE_SOURCE", 1), true),
    replenishment_status_1: _bv_text(_bv_field(ds, "REPLENISHMENT_STATUS_1", ""), ""),
    replenishment_status_2: _bv_text(_bv_field(ds, "REPLENISHMENT_STATUS_2", ""), ""),
    min_stock: _bv_num(_bv_field(ds, "MIN_STOCK", null), null),
    replenishment_moq: _bv_num(_bv_field(ds, "REPLENISHMENT_MOQ", null), null),
    vendor_moq: _bv_num(_bv_field(ds, "VENDOR_MOQ", null), null),
    current_purchase_price: _bv_num(_bv_field(ds, "CURRENT_PURCHASE_PRICE", null), null),
    qty: _bv_num(_bv_field(ds, "QTY", 0), 0),
    qty_on_hand: _bv_num(_bv_field(ds, "QTY_ON_HAND", _bv_field(ds, "QTY", 0)), 0),
    qty_reserved: _bv_num(_bv_field(ds, "QTY_RESERVED", 0), 0),
    qty_expected: _bv_num(_bv_field(ds, "QTY_EXPECTED", 0), 0),
    qty_available: _bv_num(_bv_field(ds, "QTY_AVAILABLE", _bv_field(ds, "QTY", 0)), 0),
    cost_amount: costAmount,
    value_amount: valueAmount,
    retail_value_amount: retailValueAmount,
    is_financial_doc: financialFlag === true,
    financial_impact_type: financialFlag === true ? "expense" : financialFlag === false ? "quantity_only" : "unknown",
    financial_value_amount: financialFlag === true ? financialValueAmount : 0
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_item_master_record(ds) {
  return {
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_external_id: _bv_text(_bv_field(ds, "ITEM_EXTERNAL_ID", ""), ""),
    external_id: _bv_text(_bv_field(ds, "EXTERNAL_ID", ""), ""),
    event_id: _bv_text(_bv_field(ds, "EVENT_ID", ""), ""),
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", ""), ""),
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    barcode: _bv_text(_bv_field(ds, "BARCODE", ""), ""),
    alternate_barcodes: _bv_text(_bv_field(ds, "ALTERNATE_BARCODES", ""), ""),
    softone_sotype: _bv_int(_bv_field(ds, "SOFTONE_SOTYPE", 0), 0, 0, 99999999),
    brand_external_id: _bv_text(_bv_field(ds, "BRAND_EXTERNAL_ID", ""), ""),
    brand_name: _bv_text(_bv_field(ds, "BRAND_NAME", ""), ""),
    manufacturer_code: _bv_text(_bv_field(ds, "MANUFACTURER_CODE", ""), ""),
    manufacturer_name: _bv_text(_bv_field(ds, "MANUFACTURER_NAME", ""), ""),
    preferred_supplier_ext_id: _bv_text(_bv_field(ds, "PREFERRED_SUPPLIER_EXT_ID", ""), ""),
    preferred_supplier_name: _bv_text(_bv_field(ds, "PREFERRED_SUPPLIER_NAME", ""), ""),
    vat_rate: _bv_num(_bv_field(ds, "VAT_RATE", null), null),
    vat_label: _bv_text(_bv_field(ds, "VAT_LABEL", ""), ""),
    category_1: _bv_text(_bv_field(ds, "CATEGORY_1", ""), ""),
    category_2: _bv_text(_bv_field(ds, "CATEGORY_2", ""), ""),
    category_3: _bv_text(_bv_field(ds, "CATEGORY_3", ""), ""),
    group_ext_id: _bv_text(_bv_field(ds, "GROUP_EXT_ID", ""), ""),
    group_external_id: _bv_text(_bv_field(ds, "GROUP_EXTERNAL_ID", ""), ""),
    group_name: _bv_text(_bv_field(ds, "GROUP_NAME", ""), ""),
    commercial_category: _bv_text(_bv_field(ds, "COMMERCIAL_CATEGORY", ""), ""),
    manual_order_category: _bv_text(_bv_field(ds, "MANUAL_ORDER_CATEGORY", ""), ""),
    commercial_status: _bv_text(_bv_field(ds, "COMMERCIAL_STATUS", ""), ""),
    replenishment_status_1: _bv_text(_bv_field(ds, "REPLENISHMENT_STATUS_1", ""), ""),
    replenishment_status_2: _bv_text(_bv_field(ds, "REPLENISHMENT_STATUS_2", ""), ""),
    min_stock: _bv_num(_bv_field(ds, "MIN_STOCK", null), null),
    replenishment_moq: _bv_num(_bv_field(ds, "REPLENISHMENT_MOQ", null), null),
    vendor_moq: _bv_num(_bv_field(ds, "VENDOR_MOQ", null), null),
    current_purchase_price: _bv_num(_bv_field(ds, "CURRENT_PURCHASE_PRICE", null), null),
    is_active: _bv_bool(_bv_field(ds, "IS_ACTIVE_SOURCE", 1), true),
    is_active_source: _bv_bool(_bv_field(ds, "IS_ACTIVE_SOURCE", 1), true),
    company_id: _bv_text(_bv_field(ds, "COMPANY_ID", ""), "")
  };
}

function _bv_cash_record(ds) {
  var eventId = _bv_text(_bv_field(ds, "EVENT_ID", ""), "");
  var subcategory = _bv_text(_bv_field(ds, "SUBCATEGORY", "financial_accounts"), "financial_accounts");
  var rec = {
    event_id: eventId,
    external_id: eventId,
    transaction_id: _bv_text(_bv_field(ds, "TRANSACTION_ID", eventId), eventId),
    transaction_date: _bv_text(_bv_field(ds, "DOC_DATE", ""), ""),
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", ""), ""),
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), ""),
    transaction_type: _bv_text(_bv_field(ds, "TRANSACTION_TYPE", "transfer"), "transfer"),
    source_transaction_type_id: _bv_int(_bv_field(ds, "SOURCE_TRANSACTION_TYPE_ID", 0), 0, 0, 99999999),
    entry_type: subcategory,
    subcategory: subcategory,
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    account_id: _bv_text(_bv_field(ds, "ACCOUNT_ID", ""), ""),
    counterparty_type: _bv_text(_bv_field(ds, "COUNTERPARTY_TYPE", "internal"), "internal"),
    counterparty_id: _bv_text(_bv_field(ds, "COUNTERPARTY_ID", ""), ""),
    counterparty_name: _bv_text(_bv_field(ds, "COUNTERPARTY_NAME", ""), ""),
    amount: _bv_num(_bv_field(ds, "AMOUNT_ABS", 0), 0),
    amount_raw: _bv_num(_bv_field(ds, "AMOUNT_RAW", 0), 0),
    currency: "EUR",
    reference_no: _bv_text(_bv_field(ds, "REFERENCE_NO", ""), ""),
    notes: _bv_text(_bv_field(ds, "NOTES", ""), "")
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_supplier_balance_record(ds) {
  var supplierId = _bv_text(_bv_field(ds, "SUPPLIER_EXT_ID", ""), "");
  var branchExt = _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), "");
  var balanceDate = _bv_text(_bv_field(ds, "BALANCE_DATE", ""), "");
  var rec = {
    external_id: "SUPBAL|" + supplierId + "|" + branchExt + "|" + balanceDate,
    supplier_id: supplierId,
    supplier_ext_id: supplierId,
    supplier_name: _bv_text(_bv_field(ds, "SUPPLIER_NAME", ""), ""),
    supplier_afm: _bv_text(_bv_field(ds, "SUPPLIER_AFM", ""), ""),
    branch_id: branchExt,
    branch_ext_id: branchExt,
    balance_date: balanceDate,
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", balanceDate), balanceDate),
    open_balance: _bv_num(_bv_field(ds, "OPEN_BALANCE", 0), 0),
    overdue_balance: _bv_num(_bv_field(ds, "OVERDUE_BALANCE", 0), 0),
    aging_bucket_0_30: _bv_num(_bv_field(ds, "AGING_BUCKET_0_30", 0), 0),
    aging_bucket_31_60: _bv_num(_bv_field(ds, "AGING_BUCKET_31_60", 0), 0),
    aging_bucket_61_90: _bv_num(_bv_field(ds, "AGING_BUCKET_61_90", 0), 0),
    aging_bucket_90_plus: _bv_num(_bv_field(ds, "AGING_BUCKET_90_PLUS", 0), 0),
    last_payment_date: _bv_text(_bv_field(ds, "LAST_PAYMENT_DATE", ""), ""),
    trend_vs_previous: _bv_num(_bv_field(ds, "TREND_VS_PREVIOUS", 0), 0),
    currency: "EUR",
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), "")
  };
  return _bv_attach_org_fields(rec, ds);
}

function _bv_customer_balance_record(ds) {
  var customerId = _bv_text(_bv_field(ds, "CUSTOMER_EXT_ID", ""), "");
  var branchExt = _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), "");
  var balanceDate = _bv_text(_bv_field(ds, "BALANCE_DATE", ""), "");
  var rec = {
    external_id: "CUSBAL|" + customerId + "|" + branchExt + "|" + balanceDate,
    customer_id: customerId,
    customer_ext_id: customerId,
    customer_name: _bv_text(_bv_field(ds, "CUSTOMER_NAME", ""), ""),
    customer_afm: _bv_text(_bv_field(ds, "CUSTOMER_AFM", ""), ""),
    branch_id: branchExt,
    branch_ext_id: branchExt,
    balance_date: balanceDate,
    doc_date: _bv_text(_bv_field(ds, "DOC_DATE", balanceDate), balanceDate),
    open_balance: _bv_num(_bv_field(ds, "OPEN_BALANCE", 0), 0),
    overdue_balance: _bv_num(_bv_field(ds, "OVERDUE_BALANCE", 0), 0),
    aging_bucket_0_30: _bv_num(_bv_field(ds, "AGING_BUCKET_0_30", 0), 0),
    aging_bucket_31_60: _bv_num(_bv_field(ds, "AGING_BUCKET_31_60", 0), 0),
    aging_bucket_61_90: _bv_num(_bv_field(ds, "AGING_BUCKET_61_90", 0), 0),
    aging_bucket_90_plus: _bv_num(_bv_field(ds, "AGING_BUCKET_90_PLUS", 0), 0),
    last_collection_date: _bv_text(_bv_field(ds, "LAST_COLLECTION_DATE", ""), ""),
    trend_vs_previous: _bv_num(_bv_field(ds, "TREND_VS_PREVIOUS", 0), 0),
    currency: "EUR",
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), "")
  };
  return _bv_attach_org_fields(rec, ds);
}

function _bv_expense_record(ds) {
  var eventId = _bv_text(_bv_field(ds, "EVENT_ID", ""), "");
  var rec = {
    event_id: eventId,
    external_id: "EXP|" + eventId,
    expense_date: _bv_text(_bv_field(ds, "EXPENSE_DATE", ""), ""),
    doc_date: _bv_text(_bv_field(ds, "EXPENSE_DATE", ""), ""),
    updated_at: _bv_text(_bv_field(ds, "UPDATED_AT", ""), ""),
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    supplier_ext_id: _bv_text(_bv_field(ds, "SUPPLIER_EXT_ID", ""), ""),
    supplier_name: _bv_text(_bv_field(ds, "SUPPLIER_NAME", ""), ""),
    account_id: _bv_text(_bv_field(ds, "ACCOUNT_ID", ""), ""),
    document_id: _bv_text(_bv_field(ds, "DOCUMENT_ID", ""), ""),
    document_type: _bv_text(_bv_field(ds, "DOCUMENT_TYPE", "expense"), "expense"),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    expense_category_code: _bv_text(_bv_field(ds, "EXPENSE_CATEGORY_CODE", "operational"), "operational"),
    expense_category_name: _bv_text(_bv_field(ds, "EXPENSE_CATEGORY_NAME", ""), ""),
    amount_net: _bv_num(_bv_field(ds, "AMOUNT_NET", 0), 0),
    amount_tax: _bv_num(_bv_field(ds, "AMOUNT_TAX", 0), 0),
    amount_gross: _bv_num(_bv_field(ds, "AMOUNT_GROSS", 0), 0),
    currency: "EUR"
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_query_records(sql, mapper) {
  var ds = X.GETSQLDATASET(_bv_sqlserver_read_hints(sql), null);
  return _bv_dataset_records(ds, mapper);
}

function _bv_stream_result(streamCode, records, sql, debugFlag) {
  var out = {
    success: true,
    stream_code: streamCode,
    count: records.length,
    records: records
  };
  if (debugFlag) out.sql = sql;
  return out;
}

function GetSalesDocumentsForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_sales_sql(cfg);
    var records = _bv_query_records(sql, _bv_sales_record);
    return _bv_stream_result("sales_documents", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "sales_documents",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetPurchaseDocumentsForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_purchases_sql(cfg);
    var records = _bv_query_records(sql, _bv_purchase_record);
    return _bv_stream_result("purchase_documents", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "purchase_documents",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetInventoryDocumentsForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_inventory_sql(cfg);
    var records = _bv_query_records(sql, _bv_inventory_record);
    return _bv_stream_result("inventory_documents", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "inventory_documents",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetItemMasterForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_item_master_sql(cfg);
    var records = _bv_query_records(sql, _bv_item_master_record);
    return _bv_stream_result("item_master", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "item_master",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetCashTransactionsForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_cash_sql(cfg);
    var records = _bv_query_records(sql, _bv_cash_record);
    return _bv_stream_result("cash_transactions", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "cash_transactions",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetSupplierBalancesForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_supplier_balances_sql(cfg);
    var records = _bv_query_records(sql, _bv_supplier_balance_record);
    return _bv_stream_result("supplier_balances", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "supplier_balances",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetCustomerBalancesForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_customer_balances_sql(cfg);
    var records = _bv_query_records(sql, _bv_customer_balance_record);
    return _bv_stream_result("customer_balances", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "customer_balances",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetOperatingExpensesForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_expenses_sql(cfg);
    var records = _bv_query_records(sql, _bv_expense_record);
    return _bv_stream_result("operating_expenses", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "operating_expenses",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetSupplierOrdersForBI(obj) {
  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});
    var sql = _bv_supplier_orders_sql(cfg);
    var records = _bv_query_records(sql, _bv_supplier_order_record);
    return _bv_stream_result("supplier_orders", records, sql, cfg.debug);
  } catch (e) {
    return {
      success: false,
      stream_code: "supplier_orders",
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}

function GetAllForBI(obj) {
  var out = {
    success: true,
    version: BVBI_VERSION,
    generated_at: _bv_now_iso(),
    streams: {},
    warnings: []
  };

  try {
    _bv_require_client(obj);
    var cfg = _bv_resolve_request(obj || {});

    if (cfg.includeSales) {
      out.streams.sales_documents = GetSalesDocumentsForBI(obj || {});
      if (!out.streams.sales_documents.success) out.success = false;
    }
    if (cfg.includePurchases) {
      out.streams.purchase_documents = GetPurchaseDocumentsForBI(obj || {});
      if (!out.streams.purchase_documents.success) out.success = false;
    }
    if (cfg.includeInventory) {
      out.streams.inventory_documents = GetInventoryDocumentsForBI(obj || {});
      if (!out.streams.inventory_documents.success) out.success = false;
    }
    if (cfg.includeItemMaster) {
      out.streams.item_master = GetItemMasterForBI(obj || {});
      if (!out.streams.item_master.success) out.success = false;
    }
    if (cfg.includeCashTransactions) {
      out.streams.cash_transactions = GetCashTransactionsForBI(obj || {});
      if (!out.streams.cash_transactions.success) out.success = false;
    }
    if (cfg.includeSupplierBalances) {
      out.streams.supplier_balances = GetSupplierBalancesForBI(obj || {});
      if (!out.streams.supplier_balances.success) out.success = false;
    }
    if (cfg.includeCustomerBalances) {
      out.streams.customer_balances = GetCustomerBalancesForBI(obj || {});
      if (!out.streams.customer_balances.success) out.success = false;
    }
    if (cfg.includeOperatingExpenses) {
      out.streams.operating_expenses = GetOperatingExpensesForBI(obj || {});
      if (!out.streams.operating_expenses.success) out.success = false;
    }
    if (cfg.includeSupplierOrders) {
      out.streams.supplier_orders = GetSupplierOrdersForBI(obj || {});
      if (!out.streams.supplier_orders.success) out.success = false;
    }

    if (
      !cfg.includeSales &&
      !cfg.includePurchases &&
      !cfg.includeInventory &&
      !cfg.includeItemMaster &&
      !cfg.includeCashTransactions &&
      !cfg.includeSupplierBalances &&
      !cfg.includeCustomerBalances &&
      !cfg.includeOperatingExpenses &&
      !cfg.includeSupplierOrders
    ) {
      out.warnings.push("No stream selected via include* flags.");
    }
  } catch (e) {
    out.success = false;
    out.error = _bv_text(e && e.message, _bv_text(e, "Unknown error"));
  }

  return out;
}

function BuildBoxVisioIngestPayload(obj) {
  var base = GetAllForBI(obj || {});
  if (!base.success) return base;

  var payloads = {};
  if (base.streams.sales_documents && base.streams.sales_documents.success) {
    payloads.sales_documents = {
      stream_code: "sales_documents",
      records: base.streams.sales_documents.records
    };
  }
  if (base.streams.purchase_documents && base.streams.purchase_documents.success) {
    payloads.purchase_documents = {
      stream_code: "purchase_documents",
      records: base.streams.purchase_documents.records
    };
  }
  if (base.streams.inventory_documents && base.streams.inventory_documents.success) {
    payloads.inventory_documents = {
      stream_code: "inventory_documents",
      records: base.streams.inventory_documents.records
    };
  }
  if (base.streams.item_master && base.streams.item_master.success) {
    payloads.item_master = {
      stream_code: "item_master",
      records: base.streams.item_master.records
    };
  }
  if (base.streams.cash_transactions && base.streams.cash_transactions.success) {
    payloads.cash_transactions = {
      stream_code: "cash_transactions",
      records: base.streams.cash_transactions.records
    };
  }
  if (base.streams.supplier_balances && base.streams.supplier_balances.success) {
    payloads.supplier_balances = {
      stream_code: "supplier_balances",
      records: base.streams.supplier_balances.records
    };
  }
  if (base.streams.customer_balances && base.streams.customer_balances.success) {
    payloads.customer_balances = {
      stream_code: "customer_balances",
      records: base.streams.customer_balances.records
    };
  }
  if (base.streams.operating_expenses && base.streams.operating_expenses.success) {
    payloads.operating_expenses = {
      stream_code: "operating_expenses",
      records: base.streams.operating_expenses.records
    };
  }
  if (base.streams.supplier_orders && base.streams.supplier_orders.success) {
    payloads.supplier_orders = {
      stream_code: "supplier_orders",
      records: base.streams.supplier_orders.records
    };
  }

  return {
    success: true,
    version: BVBI_VERSION,
    generated_at: base.generated_at,
    payloads: payloads
  };
}

function HealthCheckBIBridge(obj) {
  try {
    _bv_require_client(obj);
    var colsFindoc = _bv_get_columns("FINDOC");
    var colsMtrlines = _bv_get_columns("MTRLINES");
    var colsMtrdoc = _bv_get_columns("MTRDOC");
    var colsTrdr = _bv_get_columns("TRDR");
    var colsMtrl = _bv_get_columns("MTRL");
    var colsSeries = _bv_get_columns("SERIES");
    return {
      success: true,
      version: BVBI_VERSION,
      generated_at: _bv_now_iso(),
      detected_tables: {
        FINDOC: colsFindoc,
        MTRLINES: colsMtrlines,
        MTRDOC: colsMtrdoc,
        TRDR: colsTrdr,
        MTRL: colsMtrl,
        SERIES: colsSeries
      },
      defaults: {
        limit: 0,
        balanceLimit: 500,
        allowFullBalanceSync: false,
        includeSupplierBalances: false,
        includeCustomerBalances: false,
        includeSupplierOrders: false,
        salesSourceCodes: "1351,11351",
        purchaseSourceCodes: "1251,1253",
        inventorySourceCodes: "1151",
        inventoryItemSoType: 51,
        cashSourceCodes: "1261,1281,1381,1412,1413,1414,1415,1416,1481",
        supplierBalanceSourceCodes: "1251,1253,1261,1281,1412,1416,1653",
        customerBalanceSourceCodes: "1351,1381,1413",
        expenseSourceCodes: "1261,1653"
      }
    };
  } catch (e) {
    return {
      success: false,
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}
