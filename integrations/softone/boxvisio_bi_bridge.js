/*
  BoxVisio BI Bridge for SoftOne Advanced JavaScript
  Version: 1.1.0

  Purpose
  - Extract Sales, Purchases, Inventory, Cash, Balances, Expenses data directly from SoftOne tables.
  - Return API-friendly JSON records for BoxVisio BI ingestion.
  - No custom browser list dependency.

  Intended use
  - Install in SoftOne Advanced JavaScript package (example: myWS).
  - Call via:
      /s1services/JS/myWS/GetSalesDocumentsForBI
      /s1services/JS/myWS/GetPurchaseDocumentsForBI
      /s1services/JS/myWS/GetInventoryDocumentsForBI
      /s1services/JS/myWS/GetCashTransactionsForBI
      /s1services/JS/myWS/GetSupplierBalancesForBI
      /s1services/JS/myWS/GetCustomerBalancesForBI
      /s1services/JS/myWS/GetOperatingExpensesForBI
      /s1services/JS/myWS/GetAllForBI
*/

var BVBI_VERSION = "1.1.0";
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
  for (i = 0; i < list.length; i++) {
    var x = _bv_text(list[i], "").replace(/\s+/g, "");
    if (/^\d+$/.test(x)) clean.push(x);
  }

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
  r.limit = _bv_int(obj && obj.limit, 4000, 1, 20000);
  r.fromDate = _bv_norm_date(obj && obj.fromDate);
  r.toDate = _bv_norm_date(obj && obj.toDate);
  r.debug = _bv_bool(obj && obj.debug, false);

  r.includeSales = _bv_bool(obj && obj.includeSales, true);
  r.includePurchases = _bv_bool(obj && obj.includePurchases, true);
  r.includeInventory = _bv_bool(obj && obj.includeInventory, true);
  r.includeCashTransactions = _bv_bool(obj && (obj.includeCashTransactions !== undefined ? obj.includeCashTransactions : obj.includeCash), true);
  r.includeSupplierBalances = _bv_bool(obj && obj.includeSupplierBalances, true);
  r.includeCustomerBalances = _bv_bool(obj && obj.includeCustomerBalances, true);
  r.includeOperatingExpenses = _bv_bool(obj && obj.includeOperatingExpenses, true);

  r.salesSourceCodes = _bv_parse_source_codes(obj && obj.salesSourceCodes, "1351,11351");
  r.purchaseSourceCodes = _bv_parse_source_codes(obj && obj.purchaseSourceCodes, "1251,1253");
  r.inventorySourceCodes = _bv_parse_source_codes(obj && obj.inventorySourceCodes, "1151");
  r.cashSourceCodes = _bv_parse_source_codes(obj && obj.cashSourceCodes, "1381,1281,1412,1413,1414,1481");
  r.supplierBalanceSourceCodes = _bv_parse_source_codes(
    obj && obj.supplierBalanceSourceCodes,
    "1251,1253,1261,1281,1412,1416"
  );
  r.customerBalanceSourceCodes = _bv_parse_source_codes(
    obj && obj.customerBalanceSourceCodes,
    "1351,11351,1353,1381,1413"
  );
  r.expenseSourceCodes = _bv_parse_source_codes(obj && obj.expenseSourceCodes, "1261");

  return r;
}

function _bv_findoc_common_exprs() {
  return {
    findoc: _bv_col_expr("F", "FINDOC", ["FINDOC"], "F.FINDOC"),
    company: _bv_col_expr("F", "FINDOC", ["COMPANY"], "0"),
    trnDate: _bv_col_expr("F", "FINDOC", ["TRNDATE"], "F.TRNDATE"),
    updDate: _bv_col_expr("F", "FINDOC", ["UPDDATE", "UPDATED", "LASTUPDATE", "TRNDATE"], "F.TRNDATE"),
    dueDate: _bv_col_expr("F", "FINDOC", ["DUEDATE", "LPAYDATE", "TRNDATE"], "F.TRNDATE"),
    branch: _bv_col_expr("F", "FINDOC", ["BRANCH"], "0"),
    series: _bv_col_expr("F", "FINDOC", ["SERIES"], "0"),
    finCode: _bv_col_expr("F", "FINDOC", ["FINCODE", "TRNNO", "DOCNUM"], "F.FINDOC"),
    trdr: _bv_col_expr("F", "FINDOC", ["TRDR"], "0"),
    sosource: _bv_col_expr("F", "FINDOC", ["SOSOURCE"], "0"),
    soredir: _bv_col_expr("F", "FINDOC", ["SOREDIR"], "0"),
    sodtype: _bv_col_expr("F", "FINDOC", ["SODTYPE"], "0"),
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
  var lMtrl = _bv_col_expr("L", "MTRLINES", ["MTRL"], "0");
  var iName = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var lQty = _bv_col_expr("L", "MTRLINES", ["QTY1", "QTY"], "0");
  var lNet = _bv_col_expr("L", "MTRLINES", ["NETLINEVAL", "NETVAL", "NETAMNT", "LINEVAL"], "0");
  var lVat = _bv_col_expr("L", "MTRLINES", ["VATAMNT", "TAXAMNT", "FPAAMNT", "LINEVAT", "LINETAX", "LINEVATAMNT", "LINETAXAMNT"], "NULL");
  var lGross = _bv_col_expr("L", "MTRLINES", ["GROSSVAL", "SUMAMNT", "TOTVAL", "LINEGROSS"], "NULL");
  var lCost = _bv_col_expr("L", "MTRLINES", ["COSTVAL", "COSTVALUE", "LCOST", "COST", "LINEVAL"], lNet);
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
  var grossExpr = "ISNULL(" + lGross + ", ISNULL(" + lNet + ",0) + " + vatExpr + ")";

  var mdWhouse = _bv_col_expr("MD", "MTRDOC", ["WHOUSE"], "0");

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.salesSourceCodes + ")";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT TOP " +
    cfg.limit +
    " " +
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
    "CAST(ISNULL(C.CODE, " +
    c.trdr +
    ") AS VARCHAR(128)) AS CUSTOMER_EXT_ID," +
    "CAST(ISNULL(C.NAME, '') AS VARCHAR(255)) AS CUSTOMER_NAME," +
    "CAST(ISNULL(I.CODE, " +
    lMtrl +
    ") AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" +
    iName +
    ", '') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS QTY," +
    "CAST(ISNULL(" +
    lNet +
    ",0) AS FLOAT) AS NET_VALUE," +
    "CAST(" +
    grossExpr +
    " AS FLOAT) AS GROSS_VALUE," +
    "CAST(" +
    vatExpr +
    " AS FLOAT) AS VAT_AMOUNT," +
    "CAST(ISNULL(" +
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
    "LEFT JOIN TRDR C ON C.TRDR=" +
    c.trdr +
    " AND C.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRL I ON I.MTRL=" +
    lMtrl +
    " AND I.COMPANY=F.COMPANY " +
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
  var lVat = _bv_col_expr("L", "MTRLINES", ["VATAMNT", "TAXAMNT", "FPAAMNT", "LINEVAT", "LINETAX", "LINEVATAMNT", "LINETAXAMNT"], "NULL");
  var lGross = _bv_col_expr("L", "MTRLINES", ["GROSSVAL", "SUMAMNT", "TOTVAL", "LINEGROSS"], "NULL");
  var lCost = _bv_col_expr("L", "MTRLINES", ["COSTVAL", "COSTVALUE", "LCOST", "COST", "LINEVAL"], lNet);
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
  var grossExpr = "ISNULL(" + lGross + ", ISNULL(" + lNet + ",0) + " + vatExpr + ")";

  var mdWhouse = _bv_col_expr("MD", "MTRDOC", ["WHOUSE"], "0");

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.purchaseSourceCodes + ")";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT TOP " +
    cfg.limit +
    " " +
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
    "CAST(ISNULL(S.CODE, " +
    c.trdr +
    ") AS VARCHAR(128)) AS SUPPLIER_EXT_ID," +
    "CAST(ISNULL(S.NAME, '') AS VARCHAR(255)) AS SUPPLIER_NAME," +
    "CAST(ISNULL(I.CODE, " +
    lMtrl +
    ") AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" +
    iName +
    ", '') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS QTY," +
    "CAST(ISNULL(" +
    lNet +
    ",0) AS FLOAT) AS NET_VALUE," +
    "CAST(" +
    grossExpr +
    " AS FLOAT) AS GROSS_VALUE," +
    "CAST(" +
    vatExpr +
    " AS FLOAT) AS VAT_AMOUNT," +
    "CAST(ISNULL(" +
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
    "LEFT JOIN TRDR S ON S.TRDR=" +
    c.trdr +
    " AND S.COMPANY=F.COMPANY " +
    "LEFT JOIN MTRL I ON I.MTRL=" +
    lMtrl +
    " AND I.COMPANY=F.COMPANY " +
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

function _bv_inventory_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var seriesInfo = _bv_series_info_expr(c);
  var branchInfo = _bv_branch_info_expr(c);

  var lLineId = _bv_col_expr("L", "MTRLINES", ["MTRLINES", "LINENUM"], "0");
  var lMtrl = _bv_col_expr("L", "MTRLINES", ["MTRL"], "0");
  var iName = _bv_col_expr("I", "MTRL", ["NAME", "DESCR", "TITLE"], "''");
  var lQty = _bv_col_expr("L", "MTRLINES", ["QTY1", "QTY"], "0");
  var lVal = _bv_col_expr("L", "MTRLINES", ["LINEVAL", "NETLINEVAL", "NETVAL", "NETAMNT"], "0");
  var mdWhouse = _bv_col_expr("MD", "MTRDOC", ["WHOUSE"], "0");

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.inventorySourceCodes + ")";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT TOP " +
    cfg.limit +
    " " +
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
    ",'inventory_' + CAST(ISNULL(" +
    c.sosource +
    ",0) + ISNULL(" +
    c.soredir +
    ",0) AS VARCHAR(16))) AS VARCHAR(255)) AS DOCUMENT_TYPE," +
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
    "CAST(ISNULL(I.CODE, " +
    lMtrl +
    ") AS VARCHAR(128)) AS ITEM_CODE," +
    "CAST(ISNULL(" +
    iName +
    ", '') AS VARCHAR(255)) AS ITEM_NAME," +
    "CAST(ISNULL(" +
    lQty +
    ",0) AS FLOAT) AS QTY," +
    "CAST(ISNULL(" +
    lVal +
    ",0) AS FLOAT) AS VALUE_AMOUNT," +
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

function _bv_cash_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var branchInfo = _bv_branch_info_expr(c);

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.cashSourceCodes + ")";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT TOP " +
    cfg.limit +
    " " +
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
    "CAST(ISNULL(" +
    c.account +
    ",0) AS VARCHAR(128)) AS ACCOUNT_ID," +
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
    "CASE " +
    "WHEN F.SOSOURCE IN (1381,1413) THEN 'inflow' " +
    "WHEN F.SOSOURCE IN (1281,1412,1261) THEN 'outflow' " +
    "WHEN F.SOSOURCE IN (1414,1481) THEN 'transfer' " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=13 THEN 'inflow' " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=12 THEN 'outflow' " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=14 THEN 'transfer' " +
    "WHEN ISNULL(" +
    c.sumAmount +
    ",0) < 0 THEN 'outflow' " +
    "ELSE 'inflow' END AS TRANSACTION_TYPE," +
    "CASE " +
    "WHEN F.SOSOURCE=1381 THEN 'customer_collections' " +
    "WHEN F.SOSOURCE=1413 THEN 'customer_transfers' " +
    "WHEN F.SOSOURCE=1281 THEN 'supplier_payments' " +
    "WHEN F.SOSOURCE=1412 THEN 'supplier_transfers' " +
    "WHEN F.SOSOURCE IN (1414,1481) THEN 'financial_accounts' " +
    "WHEN F.SOSOURCE=1261 THEN 'supplier_payments' " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=13 THEN 'customer_collections' " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=12 THEN 'supplier_payments' " +
    "ELSE 'financial_accounts' END AS SUBCATEGORY," +
    "CASE " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=13 THEN 'customer' " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=12 THEN 'supplier' " +
    "WHEN ISNULL(" +
    c.sodtype +
    ",0)=14 THEN 'internal' " +
    "ELSE 'internal' END AS COUNTERPARTY_TYPE," +
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
    "LEFT JOIN TRDR T ON T.TRDR=" +
    c.trdr +
    " AND T.COMPANY=F.COMPANY " +
    branchInfo.joinSql +
    whereSql +
    " ORDER BY " +
    c.trnDate +
    " ASC, " +
    c.findoc +
    " ASC";

  return sql;
}

function _bv_supplier_balances_sql(cfg) {
  var c = _bv_findoc_common_exprs();
  var branchInfo = _bv_branch_info_expr(c);

  var whereSql =
    " WHERE F.COMPANY=" +
    cfg.company +
    " AND F.SODTYPE=12 AND F.SOSOURCE IN (" +
    cfg.supplierBalanceSourceCodes +
    ")";
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT TOP " +
    cfg.limit +
    " " +
    "CAST(ISNULL(T.CODE, " +
    c.trdr +
    ") AS VARCHAR(64)) AS SUPPLIER_EXT_ID," +
    "CAST(ISNULL(T.NAME, '') AS VARCHAR(255)) AS SUPPLIER_NAME," +
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
    (cfg.toDate !== "" ? _bv_sql_quote(cfg.toDate) : "GETDATE()") +
    ", 23) AS BALANCE_DATE," +
    "CAST(SUM(CASE " +
    "WHEN F.SOSOURCE IN (1251,1253,1261,1653) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) " +
    "WHEN F.SOSOURCE IN (1281,1412,1416) THEN -ABS(ISNULL(" +
    c.sumAmount +
    ",0)) " +
    "ELSE ISNULL(" +
    c.sumAmount +
    ",0) END) AS FLOAT) AS OPEN_BALANCE," +
    "CAST(SUM(CASE " +
    "WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) > 0 AND " +
    "(CASE WHEN F.SOSOURCE IN (1251,1253,1261,1653) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) WHEN F.SOSOURCE IN (1281,1412,1416) THEN -ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE ISNULL(" +
    c.sumAmount +
    ",0) END) > 0 " +
    "THEN (CASE WHEN F.SOSOURCE IN (1251,1253,1261,1653) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) WHEN F.SOSOURCE IN (1281,1412,1416) THEN -ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE ISNULL(" +
    c.sumAmount +
    ",0) END) " +
    "ELSE 0 END) AS FLOAT) AS OVERDUE_BALANCE," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) BETWEEN 0 AND 30 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1251,1253,1261,1653) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_0_30," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) BETWEEN 31 AND 60 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1251,1253,1261,1653) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_31_60," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) BETWEEN 61 AND 90 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1251,1253,1261,1653) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_61_90," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) > 90 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1251,1253,1261,1653) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_90_PLUS," +
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

  var whereSql =
    " WHERE F.COMPANY=" +
    cfg.company +
    " AND F.SODTYPE=13 AND F.SOSOURCE IN (" +
    cfg.customerBalanceSourceCodes +
    ")";
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT TOP " +
    cfg.limit +
    " " +
    "CAST(ISNULL(T.CODE, " +
    c.trdr +
    ") AS VARCHAR(64)) AS CUSTOMER_EXT_ID," +
    "CAST(ISNULL(T.NAME, '') AS VARCHAR(255)) AS CUSTOMER_NAME," +
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
    (cfg.toDate !== "" ? _bv_sql_quote(cfg.toDate) : "GETDATE()") +
    ", 23) AS BALANCE_DATE," +
    "CAST(SUM(CASE " +
    "WHEN F.SOSOURCE IN (1351,11351,1353) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) " +
    "WHEN F.SOSOURCE IN (1381,1413) THEN -ABS(ISNULL(" +
    c.sumAmount +
    ",0)) " +
    "ELSE ISNULL(" +
    c.sumAmount +
    ",0) END) AS FLOAT) AS OPEN_BALANCE," +
    "CAST(SUM(CASE " +
    "WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) > 0 AND " +
    "(CASE WHEN F.SOSOURCE IN (1351,11351,1353) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) WHEN F.SOSOURCE IN (1381,1413) THEN -ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE ISNULL(" +
    c.sumAmount +
    ",0) END) > 0 " +
    "THEN (CASE WHEN F.SOSOURCE IN (1351,11351,1353) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) WHEN F.SOSOURCE IN (1381,1413) THEN -ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE ISNULL(" +
    c.sumAmount +
    ",0) END) " +
    "ELSE 0 END) AS FLOAT) AS OVERDUE_BALANCE," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) BETWEEN 0 AND 30 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1351,11351,1353) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_0_30," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) BETWEEN 31 AND 60 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1351,11351,1353) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_31_60," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) BETWEEN 61 AND 90 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1351,11351,1353) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_61_90," +
    "CAST(SUM(CASE WHEN DATEDIFF(day, " +
    c.dueDate +
    ", GETDATE()) > 90 THEN " +
    "(CASE WHEN F.SOSOURCE IN (1351,11351,1353) THEN ABS(ISNULL(" +
    c.sumAmount +
    ",0)) ELSE 0 END) ELSE 0 END) AS FLOAT) AS AGING_BUCKET_90_PLUS," +
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

  var whereSql = " WHERE F.COMPANY=" + cfg.company + " AND F.SOSOURCE IN (" + cfg.expenseSourceCodes + ")";
  if (cfg.fromDate !== "") whereSql += " AND " + c.trnDate + " >= " + _bv_sql_quote(cfg.fromDate);
  if (cfg.toDate !== "") whereSql += " AND " + c.trnDate + " < DATEADD(day,1," + _bv_sql_quote(cfg.toDate) + ")";

  var sql =
    "SELECT TOP " +
    cfg.limit +
    " " +
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
    "CAST(ISNULL(" +
    seriesInfo.seriesNameExpr +
    ",'expense_' + CAST(ISNULL(" +
    c.sosource +
    ",0) AS VARCHAR(16))) AS VARCHAR(128)) AS DOCUMENT_TYPE," +
    "CAST(ABS(ISNULL(" +
    c.sumAmount +
    ",0)) AS FLOAT) AS AMOUNT_NET," +
    "CAST(ABS(ISNULL(" +
    c.taxAmount +
    ",0)) AS FLOAT) AS AMOUNT_TAX," +
    "CAST(ABS(ISNULL(" +
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
    whereSql +
    " ORDER BY " +
    c.trnDate +
    " ASC, " +
    c.findoc +
    " ASC";

  return sql;
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
    document_id: _bv_text(_bv_field(ds, "DOCUMENT_ID", ""), ""),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    document_series: _bv_text(_bv_field(ds, "DOCUMENT_SERIES", ""), ""),
    document_series_name: _bv_text(_bv_field(ds, "DOCUMENT_SERIES_NAME", ""), ""),
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    warehouse_ext_id: _bv_text(_bv_field(ds, "WAREHOUSE_EXT_ID", ""), ""),
    customer_ext_id: _bv_text(_bv_field(ds, "CUSTOMER_EXT_ID", ""), ""),
    customer_name: _bv_text(_bv_field(ds, "CUSTOMER_NAME", ""), ""),
    entity_ext_id: _bv_text(_bv_field(ds, "CUSTOMER_EXT_ID", ""), ""),
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    qty: _bv_num(_bv_field(ds, "QTY", 0), 0),
    net_value: _bv_num(_bv_field(ds, "NET_VALUE", 0), 0),
    gross_value: _bv_num(_bv_field(ds, "GROSS_VALUE", 0), 0),
    vat_amount: _bv_num(_bv_field(ds, "VAT_AMOUNT", 0), 0),
    cost_amount: _bv_num(_bv_field(ds, "COST_AMOUNT", 0), 0)
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
    document_type: _bv_text(_bv_field(ds, "DOCUMENT_TYPE", "purchase"), "purchase"),
    document_id: _bv_text(_bv_field(ds, "DOCUMENT_ID", ""), ""),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    document_series: _bv_text(_bv_field(ds, "DOCUMENT_SERIES", ""), ""),
    document_series_name: _bv_text(_bv_field(ds, "DOCUMENT_SERIES_NAME", ""), ""),
    branch_ext_id: _bv_text(_bv_field(ds, "BRANCH_EXT_ID", ""), ""),
    warehouse_ext_id: _bv_text(_bv_field(ds, "WAREHOUSE_EXT_ID", ""), ""),
    supplier_ext_id: _bv_text(_bv_field(ds, "SUPPLIER_EXT_ID", ""), ""),
    supplier_name: _bv_text(_bv_field(ds, "SUPPLIER_NAME", ""), ""),
    entity_ext_id: _bv_text(_bv_field(ds, "SUPPLIER_EXT_ID", ""), ""),
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    qty: _bv_num(_bv_field(ds, "QTY", 0), 0),
    net_value: _bv_num(_bv_field(ds, "NET_VALUE", 0), 0),
    gross_value: _bv_num(_bv_field(ds, "GROSS_VALUE", 0), 0),
    vat_amount: _bv_num(_bv_field(ds, "VAT_AMOUNT", 0), 0),
    cost_amount: _bv_num(_bv_field(ds, "COST_AMOUNT", 0), 0)
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_inventory_record(ds) {
  var eventId = _bv_text(_bv_field(ds, "EVENT_ID", ""), "");
  var documentType = _bv_text(_bv_field(ds, "DOCUMENT_TYPE", "inventory"), "inventory");
  var seriesName = _bv_text(_bv_field(ds, "DOCUMENT_SERIES_NAME", ""), "");
  var valueAmount = _bv_num(_bv_field(ds, "VALUE_AMOUNT", 0), 0);
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
    item_code: _bv_text(_bv_field(ds, "ITEM_CODE", ""), ""),
    item_name: _bv_text(_bv_field(ds, "ITEM_NAME", ""), ""),
    qty: _bv_num(_bv_field(ds, "QTY", 0), 0),
    value_amount: valueAmount,
    is_financial_doc: financialFlag === true,
    financial_impact_type: financialFlag === true ? "expense" : financialFlag === false ? "quantity_only" : "unknown",
    financial_value_amount: financialFlag === true ? valueAmount : 0
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
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
    branch_id: branchExt,
    branch_ext_id: branchExt,
    balance_date: balanceDate,
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
    branch_id: branchExt,
    branch_ext_id: branchExt,
    balance_date: balanceDate,
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
    document_type: _bv_text(_bv_field(ds, "DOCUMENT_TYPE", "expense"), "expense"),
    document_no: _bv_text(_bv_field(ds, "DOCUMENT_NO", ""), ""),
    expense_category_code: "operational",
    amount_net: _bv_num(_bv_field(ds, "AMOUNT_NET", 0), 0),
    amount_tax: _bv_num(_bv_field(ds, "AMOUNT_TAX", 0), 0),
    amount_gross: _bv_num(_bv_field(ds, "AMOUNT_GROSS", 0), 0),
    currency: "EUR"
  };
  return _bv_attach_org_fields(_bv_attach_common_doc_fields(rec, ds), ds);
}

function _bv_query_records(sql, mapper) {
  var ds = X.GETSQLDATASET(sql, null);
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

    if (
      !cfg.includeSales &&
      !cfg.includePurchases &&
      !cfg.includeInventory &&
      !cfg.includeCashTransactions &&
      !cfg.includeSupplierBalances &&
      !cfg.includeCustomerBalances &&
      !cfg.includeOperatingExpenses
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
        salesSourceCodes: "1351,11351",
        purchaseSourceCodes: "1251,1253",
        inventorySourceCodes: "1151",
        cashSourceCodes: "1381,1281,1412,1413,1414,1481",
        supplierBalanceSourceCodes: "1251,1253,1261,1281,1412,1416",
        customerBalanceSourceCodes: "1351,11351,1353,1381,1413",
        expenseSourceCodes: "1261"
      }
    };
  } catch (e) {
    return {
      success: false,
      error: _bv_text(e && e.message, _bv_text(e, "Unknown error"))
    };
  }
}
