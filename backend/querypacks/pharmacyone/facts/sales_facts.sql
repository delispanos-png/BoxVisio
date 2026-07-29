SELECT
  CAST(F.TRNDATE AS date) AS doc_date,
  CAST(CAST(ISNULL(F.COMPANY, 0) AS nvarchar(32)) + ':' + CAST(ISNULL(F.BRANCH, 0) AS nvarchar(32)) AS nvarchar(64)) AS branch_external_id,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_external_id,
  CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(64)) AS warehouse_external_id,
  CAST(NULL AS nvarchar(64)) AS supplier_external_id,
  CAST(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    )
    * COALESCE(TRY_CAST(ISNULL(L.QTY1, ISNULL(L.QTY, 0)) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS qty,
  CAST(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    )
    * COALESCE(TRY_CAST(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS net_amount,
  CAST(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    )
    * COALESCE(TRY_CAST(ISNULL(NULLIF(L.SALESCVAL,0), ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0))) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS cost_amount,
  CAST('S|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS external_id,
  CAST(ISNULL(F.UPDDATE, F.TRNDATE) AS datetime2) AS updated_at,
  CAST(ISNULL(F.SOTIME, F.INSDATE) AS datetime2) AS source_created_at,
  CAST(NULLIF(CAST(ISNULL(I.MTRMARK, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS brand_external_id,
  CAST(ISNULL(MK.NAME, '') AS nvarchar(255)) AS brand_name,
  CAST(NULL AS nvarchar(64)) AS category_external_id,
  CAST(NULLIF(CAST(ISNULL(I.MTRGROUP, 0) AS nvarchar(64)), '0') AS nvarchar(64)) AS group_external_id,

  CAST(CAST(F.FINDOC AS nvarchar(40)) + '-' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) AS event_id,
  CAST(F.FINDOC AS nvarchar(40)) AS document_id,
  CAST(ISNULL(F.FINCODE, F.FINDOC) AS nvarchar(128)) AS document_no,
  CAST(F.SERIES AS nvarchar(128)) AS document_series,
  CAST(ISNULL(SR.NAME, CAST(F.SERIES AS nvarchar(255))) AS nvarchar(255)) AS document_series_name,
  CAST('sales_' + CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS nvarchar(16)) AS nvarchar(128)) AS document_type,
  CAST(
    CASE
      WHEN ISNULL(F.ISCANCEL, 0) = 1 THEN N'Cancelled'
      WHEN ISNULL(F.FINSTATES, 0) IN (10) THEN N'Completed'
      WHEN ISNULL(F.FINSTATES, 0) IN (3, 4, 6) THEN N'Closed'
      WHEN ISNULL(F.FINSTATES, 0) IN (1, 2) THEN N'Open'
      ELSE N'Open'
    END AS nvarchar(128)
  ) AS document_status,
  CAST(ISNULL(C.CODE, F.TRDR) AS nvarchar(128)) AS customer_ext_id,
  CAST(ISNULL(C.NAME, '') AS nvarchar(255)) AS customer_name,
  CAST(ISNULL(C.AFM, '') AS nvarchar(64)) AS customer_afm,
  CAST(ISNULL(I.CODE, L.MTRL) AS nvarchar(128)) AS item_code,
  CAST(ISNULL(I.NAME, '') AS nvarchar(255)) AS item_name,
  CAST(ISNULL(MG.NAME, '') AS nvarchar(255)) AS group_name,
  CAST(COALESCE(NULLIF(UT4.NAME, ''), NULLIF(UT4.CODE, ''), NULLIF(CAST(IX.UTBL04 AS nvarchar(128)), '0'), '') AS nvarchar(128)) AS manual_order_category,
  CAST(COALESCE(NULLIF(UT5.NAME, ''), NULLIF(UT5.CODE, ''), NULLIF(CAST(IX.UTBL05 AS nvarchar(128)), '0'), '') AS nvarchar(128)) AS commercial_status,
  CAST(0 AS decimal(18,6)) AS discount_pct,
  CAST(0 AS decimal(28,8)) AS discount_amount,

  CAST(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    )
    * COALESCE(TRY_CAST(ISNULL(L.VATAMNT, 0) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS vat_amount,
  CAST(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    )
    * (
      COALESCE(TRY_CAST(ISNULL(L.NETLINEVAL, ISNULL(L.LINEVAL, 0)) AS decimal(28,8)), 0)
      + COALESCE(TRY_CAST(ISNULL(L.VATAMNT, 0) AS decimal(28,8)), 0)
    )
    AS decimal(28,8)
  ) AS gross_value,
  CAST(COALESCE(TRY_CAST(ISNULL(F.NETAMNT, 0) AS decimal(28,8)), 0) AS decimal(28,8)) AS doc_net_total,
  CAST(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    )
    * COALESCE(TRY_CAST(ISNULL(F.EXPN, 0) AS decimal(28,8)), 0)
    AS decimal(28,8)
  ) AS doc_expenses_total,
  CAST(COALESCE(TRY_CAST(ISNULL(F.VATAMNT, 0) AS decimal(28,8)), 0) AS decimal(28,8)) AS doc_tax_total,
  CAST(COALESCE(TRY_CAST(ISNULL(F.SUMAMNT, 0) AS decimal(28,8)), 0) AS decimal(28,8)) AS doc_gross_total,

  CAST(
    COALESCE(
      NULLIF(CAST(ISNULL(F.CCC88ECHANNEL, 0) AS nvarchar(64)), '0'),
      NULLIF(CAST(ISNULL(ORIG.CCC88ECHANNEL, 0) AS nvarchar(64)), '0'),
      CASE
        WHEN COALESCE(
          NULLIF(LTRIM(RTRIM(ISNULL(F.CCC88EORDERNO, ''))), ''),
          NULLIF(LTRIM(RTRIM(ISNULL(ORIG.CCC88EORDERNO, ''))), '')
        ) IS NOT NULL THEN '1'
        ELSE NULL
      END
    ) AS nvarchar(64)
  ) AS channel_ext_id,
  CAST(
    COALESCE(
      NULLIF(EC.NAME, ''),
      CASE
        WHEN COALESCE(
          NULLIF(LTRIM(RTRIM(ISNULL(F.CCC88EORDERNO, ''))), ''),
          NULLIF(LTRIM(RTRIM(ISNULL(ORIG.CCC88EORDERNO, ''))), '')
        ) IS NOT NULL THEN N'Site'
        ELSE N''
      END
    ) AS nvarchar(255)
  ) AS channel_name,
  CAST(COALESCE(NULLIF(F.CCC88EORDERNO, ''), NULLIF(ORIG.CCC88EORDERNO, ''), '') AS nvarchar(128)) AS eshop_code,
  CAST(
    COALESCE(
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPPINGMETHOD'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPMENTMETHOD'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.DISPATCHMETHOD'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SENDMETHOD'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPPING'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPMENT'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.DELIVMODE'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPMENTMODE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPPINGMETHOD'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPMENTMETHOD'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.DISPATCHMETHOD'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SENDMETHOD'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPPING'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPMENT'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.DELIVMODE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPMENTMODE'), ''),
      ''
    ) AS nvarchar(128)
  ) AS shipping_method,
  CAST(
    COALESCE(
      NULLIF(ORIGPM.NAME, ''),
      NULLIF(ORIGPM.CODE, ''),
      NULLIF(CAST(ORIG.PAYMENT AS nvarchar(128)), '0'),
      NULLIF(PM.NAME, ''),
      NULLIF(PM.CODE, ''),
      NULLIF(CAST(F.PAYMENT AS nvarchar(128)), '0'),
      ''
    ) AS nvarchar(128)
  ) AS payment_method,
  CAST(COALESCE(NULLIF(F.COMMENTS, ''), NULLIF(ORIG.COMMENTS, ''), NULLIF(ORIG.CCC88EORDERCOM, ''), '') AS nvarchar(255)) AS reason,
  CAST(ISNULL(ORIG.FINCODE, '') AS nvarchar(128)) AS origin_ref,
  CAST(ISNULL(F.FINCODE, '') AS nvarchar(128)) AS destination_ref,
  CAST(COALESCE(NULLIF(ORIGCB.NAME, ''), NULLIF(CB.NAME, ''), '') AS nvarchar(255)) AS customer_branch,
  CAST(
    COALESCE(
      NULLIF(ORIGMD.SHIPPINGADDR, ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPADRESS'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPADDRESS'), ''),
      NULLIF(MD.SHIPPINGADDR, ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPADRESS'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPADDRESS'), ''),
      ''
    ) AS nvarchar(1024)
  ) AS delivery_address,
  CAST(
    COALESCE(
      NULLIF(ORIGMD.SHPZIP, ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPZIP'), ''),
      NULLIF(MD.SHPZIP, ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPZIP'), ''),
      ''
    ) AS nvarchar(32)
  ) AS delivery_zip,
  CAST(
    COALESCE(
      NULLIF(ORIGMD.SHPCITY, ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPCITY'), ''),
      NULLIF(MD.SHPCITY, ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPCITY'), ''),
      ''
    ) AS nvarchar(128)
  ) AS delivery_city,
  CAST(
    COALESCE(
      NULLIF(ORIGMD.SHPDISTRICT, ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPAREA'), ''),
      NULLIF(MD.SHPDISTRICT, ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPAREA'), ''),
      ''
    ) AS nvarchar(128)
  ) AS delivery_area,
  CAST(
    COALESCE(
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.MTRDOCMOVNAME'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.MOVEMENTTYPE'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.MOVTYPE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.MTRDOCMOVNAME'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.MOVEMENTTYPE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.MOVTYPE'), ''),
      ''
    ) AS nvarchar(128)
  ) AS movement_type,
  CAST(COALESCE(NULLIF(ORIG.CCC88EMRKTCOUR, ''), NULLIF(F.CCC88EMRKTCOUR, ''), '') AS nvarchar(255)) AS carrier_name,
  CAST(
    COALESCE(
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.SHIPMENTMODE'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.TRANSPORTMEANS'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.VEHICLETYPE'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.CCCVEHICLETYPE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.SHIPMENTMODE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.TRANSPORTMEANS'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.VEHICLETYPE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.CCCVEHICLETYPE'), ''),
      ''
    ) AS nvarchar(128)
  ) AS transport_medium,
  CAST(
    COALESCE(
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.VEHICLENO'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.TRANSPORTNO'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.PLATENO'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.CARNO'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.VEHICLENO'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.TRANSPORTNO'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.PLATENO'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.CARNO'), ''),
      ''
    ) AS nvarchar(128)
  ) AS transport_no,
  CAST(
    COALESCE(
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.ROUTENAME'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.ITINERARY'), ''),
      NULLIF(JSON_VALUE(ORIGJ.payload, '$.ROUTE'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.ROUTENAME'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.ITINERARY'), ''),
      NULLIF(JSON_VALUE(FJ.payload, '$.ROUTE'), ''),
      ''
    ) AS nvarchar(255)
  ) AS route_name,
  CAST(COALESCE(TRY_CAST(JSON_VALUE(ORIGJ.payload, '$.LOADINGDATE') AS date), TRY_CAST(JSON_VALUE(FJ.payload, '$.LOADINGDATE') AS date)) AS date) AS loading_date,
  CAST(
    COALESCE(
      TRY_CAST(JSON_VALUE(ORIGJ.payload, '$.DELIVERYDATE') AS date),
      TRY_CAST(JSON_VALUE(ORIGJ.payload, '$.SHIPDATE') AS date),
      TRY_CAST(JSON_VALUE(FJ.payload, '$.DELIVERYDATE') AS date),
      TRY_CAST(JSON_VALUE(FJ.payload, '$.SHIPDATE') AS date)
    ) AS date
  ) AS delivery_date,
  CAST(COALESCE(NULLIF(ORIG.CCC88EVOUCHER, ''), NULLIF(F.CCC88EVOUCHER, ''), '') AS nvarchar(128)) AS voucher_no,
  CAST(COALESCE(NULLIF(ORIG.CCC88EVCHURL, ''), NULLIF(F.CCC88EVCHURL, ''), '') AS nvarchar(1024)) AS voucher_url,
  CAST(COALESCE(NULLIF(ORIG.CCC88ELOCKERID, ''), NULLIF(F.CCC88ELOCKERID, ''), '') AS nvarchar(128)) AS locker_id,
  CAST(COALESCE(NULLIF(ORIG.CCC88EORDERCOM, ''), NULLIF(F.CCC88EORDERCOM, ''), '') AS nvarchar(1024)) AS order_comments,
  CAST(COALESCE(NULLIF(ORIG.CCC88ESHIPCOM, ''), NULLIF(F.CCC88ESHIPCOM, ''), '') AS nvarchar(1024)) AS shipping_comments,
  CAST(COALESCE(NULLIF(ORIG.CCC88EGIFTCOM, ''), NULLIF(F.CCC88EGIFTCOM, ''), '') AS nvarchar(1024)) AS gift_comments,
  CAST(COALESCE(NULLIF(ORIG.CCC88EMRKTCOUR, ''), NULLIF(F.CCC88EMRKTCOUR, ''), '') AS nvarchar(255)) AS marketplace_courier,
  CAST(COALESCE(NULLIF(ORIG.CCC88ELOGIKAID, ''), NULLIF(F.CCC88ELOGIKAID, ''), '') AS nvarchar(128)) AS marketplace_internal_id,
  CAST(COALESCE(NULLIF(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    ) * COALESCE(TRY_CAST(ISNULL(F.EXPN, 0) AS decimal(28,8)), 0),
    0
  ), EXA.shipping_expense_value, 0) AS decimal(28,8)) AS shipping_expense_value,
  CAST(ISNULL(EXA.shipping_expense_description, '') AS nvarchar(1024)) AS shipping_expense_description,
  CAST(COALESCE(NULLIF(
    (
      CASE
        WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
        WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
        ELSE 1
      END
    ) * COALESCE(TRY_CAST(ISNULL(F.EXPN, 0) AS decimal(28,8)), 0),
    0
  ), EXA.charge_revenue_net_value, 0) AS decimal(28,8)) AS charge_revenue_net_value,
  CAST(COALESCE(EXA.charge_revenue_vat_value, 0) AS decimal(28,8)) AS charge_revenue_vat_value,
  CAST(COALESCE(
    NULLIF(
      (
        (
          CASE
            WHEN ISNULL(F.SOSOURCE, 0) = 1351 AND ISNULL(F.TFPRMS, 0) IN (151, 152, 181) THEN -1
            WHEN ISNULL(F.SOSOURCE, 0) <> 1351 AND ISNULL(F.TFPRMS, 0) IN (102, 181) THEN -1
            ELSE 1
          END
        ) * COALESCE(TRY_CAST(ISNULL(F.EXPN, 0) AS decimal(28,8)), 0)
      ) + COALESCE(EXA.charge_revenue_vat_value, 0),
      0
    ),
    EXA.charge_revenue_gross_value,
    0
  ) AS decimal(28,8)) AS charge_revenue_gross_value,
  CAST(ISNULL(EXA.charge_revenue_description, '') AS nvarchar(1024)) AS charge_revenue_description,
  CAST(ISNULL(EXA.charge_revenue_lines_json, N'[]') AS nvarchar(max)) AS charge_revenue_lines_json,
  CAST(COALESCE(EXA.shipping_charge_net_value, 0) AS decimal(28,8)) AS shipping_charge_net_value,
  CAST(COALESCE(EXA.shipping_charge_vat_value, 0) AS decimal(28,8)) AS shipping_charge_vat_value,
  CAST(COALESCE(EXA.shipping_charge_gross_value, 0) AS decimal(28,8)) AS shipping_charge_gross_value,
  CAST(COALESCE(EXA.cod_charge_net_value, 0) AS decimal(28,8)) AS cod_charge_net_value,
  CAST(COALESCE(EXA.cod_charge_vat_value, 0) AS decimal(28,8)) AS cod_charge_vat_value,
  CAST(COALESCE(EXA.cod_charge_gross_value, 0) AS decimal(28,8)) AS cod_charge_gross_value,
  CAST(COALESCE(EXA.gift_charge_net_value, 0) AS decimal(28,8)) AS gift_charge_net_value,
  CAST(COALESCE(EXA.gift_charge_vat_value, 0) AS decimal(28,8)) AS gift_charge_vat_value,
  CAST(COALESCE(EXA.gift_charge_gross_value, 0) AS decimal(28,8)) AS gift_charge_gross_value,
  CAST(COALESCE(EXA.other_charge_net_value, 0) AS decimal(28,8)) AS other_charge_net_value,
  CAST(COALESCE(EXA.other_charge_vat_value, 0) AS decimal(28,8)) AS other_charge_vat_value,
  CAST(COALESCE(EXA.other_charge_gross_value, 0) AS decimal(28,8)) AS other_charge_gross_value,
  CAST(ISNULL(F.SOSOURCE, 0) AS int) AS source_module_id,
  CAST(ISNULL(F.SOREDIR, 0) AS int) AS redirect_module_id,
  CAST(ISNULL(F.SODTYPE, 0) AS int) AS source_entity_id,
  CAST(ISNULL(F.TFPRMS, 0) AS int) AS source_transaction_type_id,
  CAST(ISNULL(F.SOSOURCE, 0) + ISNULL(F.SOREDIR, 0) AS int) AS object_id,
  CAST(ISNULL(BR.NAME, CAST(F.BRANCH AS nvarchar(255))) AS nvarchar(255)) AS branch_name,
  CAST(ISNULL(WH.NAME, CAST(ISNULL(MD.WHOUSE, 0) AS nvarchar(255))) AS nvarchar(255)) AS warehouse_name,
  CAST(F.BRANCH AS nvarchar(64)) AS branch_code,
  CAST(F.COMPANY AS nvarchar(64)) AS company_id
FROM FINDOC F WITH (NOLOCK)
OUTER APPLY (
  SELECT (
    SELECT F.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
  ) AS payload
) FJ
INNER JOIN MTRLINES L WITH (NOLOCK) ON L.FINDOC = F.FINDOC AND L.COMPANY = F.COMPANY
OUTER APPLY (
  SELECT TOP 1
    OFD.FINDOC,
    OFD.FINCODE,
    OFD.TRNDATE,
    OFD.SOSOURCE,
    OFD.SOREDIR,
    OFD.SODTYPE,
    OFD.TFPRMS,
    OFD.CCC88ECHANNEL,
    OFD.CCC88EORDERNO,
    OFD.CCC88EORDERCOM,
    OFD.CCC88ESHIPCOM,
    OFD.CCC88EGIFTCOM,
    OFD.CCC88EPMNTCODE,
    OFD.CCC88ELOCKERID,
    OFD.CCC88EVOUCHER,
    OFD.CCC88EVCHURL,
    OFD.CCC88EMRKTCOUR,
    OFD.CCC88ELOGIKAID,
    OFD.PAYMENT,
    OFD.COMMENTS,
    OFD.TRDBRANCH
  FROM FINDOC OFD WITH (NOLOCK)
  WHERE OFD.FINDOC = NULLIF(L.FINDOCS, 0) AND OFD.COMPANY = L.COMPANY
) ORIG
OUTER APPLY (
  SELECT (
    SELECT ORIG.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
  ) AS payload
) ORIGJ
OUTER APPLY (
  SELECT TOP 1
    MD.WHOUSE,
    MD.SHIPPINGADDR,
    MD.SHPZIP,
    MD.SHPDISTRICT,
    MD.SHPCITY
  FROM MTRDOC MD WITH (NOLOCK)
  WHERE MD.FINDOC = F.FINDOC AND MD.COMPANY = F.COMPANY
) MD
OUTER APPLY (
  SELECT TOP 1
    OMD.WHOUSE,
    OMD.SHIPPINGADDR,
    OMD.SHPZIP,
    OMD.SHPDISTRICT,
    OMD.SHPCITY
  FROM MTRDOC OMD WITH (NOLOCK)
  WHERE OMD.FINDOC = ORIG.FINDOC AND OMD.COMPANY = F.COMPANY
) ORIGMD
LEFT JOIN WHOUSE WH WITH (NOLOCK) ON WH.WHOUSE = MD.WHOUSE AND WH.COMPANY = F.COMPANY
LEFT JOIN TRDR C WITH (NOLOCK) ON C.TRDR = F.TRDR AND C.COMPANY = F.COMPANY
LEFT JOIN MTRL I WITH (NOLOCK) ON I.MTRL = L.MTRL AND I.COMPANY = F.COMPANY
LEFT JOIN MTREXTRA IX WITH (NOLOCK) ON IX.MTRL = I.MTRL AND IX.COMPANY = I.COMPANY
LEFT JOIN UTBL04 UT4 WITH (NOLOCK) ON UT4.UTBL04 = IX.UTBL04 AND UT4.COMPANY = IX.COMPANY AND UT4.SODTYPE = I.SODTYPE
LEFT JOIN UTBL05 UT5 WITH (NOLOCK) ON UT5.UTBL05 = IX.UTBL05 AND UT5.COMPANY = IX.COMPANY AND UT5.SODTYPE = I.SODTYPE
LEFT JOIN MTRMARK MK WITH (NOLOCK) ON MK.MTRMARK = I.MTRMARK AND MK.COMPANY = I.COMPANY
LEFT JOIN MTRGROUP MG WITH (NOLOCK) ON MG.MTRGROUP = I.MTRGROUP AND MG.COMPANY = I.COMPANY
LEFT JOIN BRANCH BR WITH (NOLOCK) ON BR.BRANCH = F.BRANCH AND BR.COMPANY = F.COMPANY
LEFT JOIN TRDBRANCH CB WITH (NOLOCK) ON CB.TRDBRANCH = F.TRDBRANCH AND (CB.COMPANY = F.COMPANY OR CB.COMPANY = 0)
LEFT JOIN TRDBRANCH ORIGCB WITH (NOLOCK) ON ORIGCB.TRDBRANCH = ORIG.TRDBRANCH AND (ORIGCB.COMPANY = F.COMPANY OR ORIGCB.COMPANY = 0)
LEFT JOIN SERIES SR WITH (NOLOCK) ON SR.SERIES = F.SERIES AND SR.COMPANY = F.COMPANY AND SR.SOSOURCE = F.SOSOURCE
OUTER APPLY (
  SELECT
    CAST(
      COALESCE(SUM(exp_net_value), 0) AS decimal(28,8)
    ) AS shipping_expense_value,
    CAST(
      COALESCE(SUM(exp_net_value), 0) AS decimal(28,8)
    ) AS charge_revenue_net_value,
    CAST(
      COALESCE(SUM(exp_vat_value), 0) AS decimal(28,8)
    ) AS charge_revenue_vat_value,
    CAST(
      COALESCE(SUM(exp_gross_value), 0) AS decimal(28,8)
    ) AS charge_revenue_gross_value,
    CAST(
      ISNULL(
        STUFF((
          SELECT N' | ' + expense_name
          FROM (
            SELECT DISTINCT
              NULLIF(LTRIM(RTRIM(COALESCE(
                EN2.NAME,
                JSON_VALUE(EAJ.payload, '$.EXPNAME'),
                JSON_VALUE(EAJ.payload, '$.NAME'),
                JSON_VALUE(EAJ.payload, '$.DESCR'),
                JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
                JSON_VALUE(EAJ.payload, '$.TITLE'),
                JSON_VALUE(EAJ.payload, '$.EXPENSE'),
                JSON_VALUE(EAJ.payload, '$.EXPENSES')
              ))), '') AS expense_name
            FROM EXPANAL EA2 WITH (NOLOCK)
            LEFT JOIN EXPN EN2 WITH (NOLOCK) ON EN2.EXPN = EA2.EXPN AND EN2.COMPANY = EA2.COMPANY
            OUTER APPLY (
              SELECT (
                SELECT EA2.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
              ) AS payload
            ) EAJ
            WHERE EA2.FINDOC = F.FINDOC AND EA2.COMPANY = F.COMPANY
          ) expense_names
          WHERE expense_name IS NOT NULL
          FOR XML PATH(''), TYPE
        ).value('.', 'nvarchar(max)'), 1, 3, ''),
        N''
      ) AS nvarchar(1024)
    ) AS shipping_expense_description,
    CAST(
      ISNULL(
        STUFF((
          SELECT N' | ' + expense_name
          FROM (
            SELECT DISTINCT
              NULLIF(LTRIM(RTRIM(COALESCE(
                EN2.NAME,
                JSON_VALUE(EAJ.payload, '$.EXPNAME'),
                JSON_VALUE(EAJ.payload, '$.NAME'),
                JSON_VALUE(EAJ.payload, '$.DESCR'),
                JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
                JSON_VALUE(EAJ.payload, '$.TITLE'),
                JSON_VALUE(EAJ.payload, '$.EXPENSE'),
                JSON_VALUE(EAJ.payload, '$.EXPENSES')
              ))), '') AS expense_name
            FROM EXPANAL EA2 WITH (NOLOCK)
            LEFT JOIN EXPN EN2 WITH (NOLOCK) ON EN2.EXPN = EA2.EXPN AND EN2.COMPANY = EA2.COMPANY
            OUTER APPLY (
              SELECT (
                SELECT EA2.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
              ) AS payload
            ) EAJ
            WHERE EA2.FINDOC = F.FINDOC AND EA2.COMPANY = F.COMPANY
          ) expense_names
          WHERE expense_name IS NOT NULL
          FOR XML PATH(''), TYPE
        ).value('.', 'nvarchar(max)'), 1, 3, ''),
        N''
      ) AS nvarchar(1024)
    ) AS charge_revenue_description,
    CAST(ISNULL((
      SELECT EA2.*
      FROM EXPANAL EA2 WITH (NOLOCK)
      WHERE EA2.FINDOC = F.FINDOC AND EA2.COMPANY = F.COMPANY
      FOR JSON PATH
    ), N'[]') AS nvarchar(max)) AS charge_revenue_lines_json,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'shipping' THEN exp_net_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS shipping_charge_net_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'shipping' THEN exp_vat_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS shipping_charge_vat_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'shipping' THEN exp_gross_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS shipping_charge_gross_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'cod' THEN exp_net_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS cod_charge_net_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'cod' THEN exp_vat_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS cod_charge_vat_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'cod' THEN exp_gross_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS cod_charge_gross_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'gift' THEN exp_net_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS gift_charge_net_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'gift' THEN exp_vat_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS gift_charge_vat_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'gift' THEN exp_gross_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS gift_charge_gross_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'other' THEN exp_net_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS other_charge_net_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'other' THEN exp_vat_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS other_charge_vat_value,
    CAST(
      COALESCE(SUM(CASE WHEN expense_group = 'other' THEN exp_gross_value ELSE 0 END), 0) AS decimal(28,8)
    ) AS other_charge_gross_value
  FROM (
    SELECT
      NULLIF(LTRIM(RTRIM(COALESCE(
        EN.NAME,
        JSON_VALUE(EAJ.payload, '$.EXPNAME'),
        JSON_VALUE(EAJ.payload, '$.NAME'),
        JSON_VALUE(EAJ.payload, '$.DESCR'),
        JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
        JSON_VALUE(EAJ.payload, '$.TITLE'),
        JSON_VALUE(EAJ.payload, '$.EXPENSE'),
        JSON_VALUE(EAJ.payload, '$.EXPENSES')
      ))), '') AS expense_name,
      LOWER(COALESCE(
        NULLIF(LTRIM(RTRIM(COALESCE(
          EN.NAME,
          JSON_VALUE(EAJ.payload, '$.EXPNAME'),
          JSON_VALUE(EAJ.payload, '$.NAME'),
          JSON_VALUE(EAJ.payload, '$.DESCR'),
          JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
          JSON_VALUE(EAJ.payload, '$.TITLE'),
          JSON_VALUE(EAJ.payload, '$.EXPENSE'),
          JSON_VALUE(EAJ.payload, '$.EXPENSES')
        ))), ''),
        ''
      )) AS expense_name_norm,
      COALESCE(
        TRY_CAST(
          COALESCE(
            NULLIF(CAST(EA.EXPVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.TEXPVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.LEXPVAL AS nvarchar(64)), ''),
            '0'
          ) AS decimal(28,8)
        ),
        0
      ) AS exp_net_value,
      COALESCE(
        TRY_CAST(
          COALESCE(
            NULLIF(CAST(EA.EXPVATVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.TEXPVATVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.LEXPVATVAL AS nvarchar(64)), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VATAMNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VATVAL'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.FPAVAL'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VAT_AMOUNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.TAX_AMOUNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.FPA_AMOUNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VAT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.FPA'), ''),
            '0'
          ) AS decimal(28,8)
        ),
        0
      ) AS exp_vat_value,
      COALESCE(
        TRY_CAST(
          COALESCE(
            NULLIF(CAST(EA.EXPVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.TEXPVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.LEXPVAL AS nvarchar(64)), ''),
            '0'
          ) AS decimal(28,8)
        ),
        0
      )
      + COALESCE(
        TRY_CAST(
          COALESCE(
            NULLIF(CAST(EA.EXPVATVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.TEXPVATVAL AS nvarchar(64)), ''),
            NULLIF(CAST(EA.LEXPVATVAL AS nvarchar(64)), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VATAMNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VATVAL'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.FPAVAL'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VAT_AMOUNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.TAX_AMOUNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.FPA_AMOUNT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.VAT'), ''),
            NULLIF(JSON_VALUE(EAJ.payload, '$.FPA'), ''),
            '0'
          ) AS decimal(28,8)
        ),
        0
      ) AS exp_gross_value,
      CASE
        WHEN LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE N'%αντικαταβολ%' OR LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE '%cod%' THEN 'cod'
        WHEN LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE N'%δωρ%' OR LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE '%gift%' THEN 'gift'
        WHEN LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE N'%αποστολ%' OR LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE N'%μεταφορ%' OR LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE '%courier%' OR LOWER(COALESCE(
          NULLIF(LTRIM(RTRIM(COALESCE(
            EN.NAME,
            JSON_VALUE(EAJ.payload, '$.EXPNAME'),
            JSON_VALUE(EAJ.payload, '$.NAME'),
            JSON_VALUE(EAJ.payload, '$.DESCR'),
            JSON_VALUE(EAJ.payload, '$.DESCRIPTION'),
            JSON_VALUE(EAJ.payload, '$.TITLE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSE'),
            JSON_VALUE(EAJ.payload, '$.EXPENSES')
          ))), ''),
          ''
        )) LIKE '%ship%' THEN 'shipping'
        ELSE 'other'
      END AS expense_group
    FROM EXPANAL EA WITH (NOLOCK)
    LEFT JOIN EXPN EN WITH (NOLOCK) ON EN.EXPN = EA.EXPN AND EN.COMPANY = EA.COMPANY
    OUTER APPLY (
      SELECT (
        SELECT EA.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
      ) AS payload
    ) EAJ
    WHERE EA.FINDOC = F.FINDOC AND EA.COMPANY = F.COMPANY
  ) EX
) EXA
OUTER APPLY (
  SELECT TOP 1 E.CCC88ECHANNEL, E.NAME
  FROM CCC88ECHANNEL E WITH (NOLOCK)
  WHERE E.CCC88ECHANNEL = TRY_CAST(
      COALESCE(
        NULLIF(CAST(ISNULL(F.CCC88ECHANNEL, 0) AS nvarchar(64)), '0'),
        NULLIF(CAST(ISNULL(ORIG.CCC88ECHANNEL, 0) AS nvarchar(64)), '0'),
        CASE
          WHEN COALESCE(
            NULLIF(LTRIM(RTRIM(ISNULL(F.CCC88EORDERNO, ''))), ''),
            NULLIF(LTRIM(RTRIM(ISNULL(ORIG.CCC88EORDERNO, ''))), '')
          ) IS NOT NULL THEN '1'
          ELSE NULL
        END
      ) AS int
    )
    AND (E.COMPANY = F.COMPANY OR E.COMPANY = 1001)
  ORDER BY CASE WHEN E.COMPANY = F.COMPANY THEN 0 ELSE 1 END
) EC
OUTER APPLY (
  SELECT TOP 1 OP.CODE, OP.NAME
  FROM PAYMENT OP WITH (NOLOCK)
  WHERE OP.PAYMENT = ORIG.PAYMENT
    AND OP.SODTYPE = ORIG.SODTYPE
    AND (OP.COMPANY = F.COMPANY OR OP.COMPANY = 1000)
  ORDER BY CASE WHEN OP.COMPANY = F.COMPANY THEN 0 ELSE 1 END
) ORIGPM
OUTER APPLY (
  SELECT TOP 1 P.CODE, P.NAME
  FROM PAYMENT P WITH (NOLOCK)
  WHERE P.PAYMENT = F.PAYMENT
    AND P.SODTYPE = F.SODTYPE
    AND (P.COMPANY = F.COMPANY OR P.COMPANY = 1000)
  ORDER BY CASE WHEN P.COMPANY = F.COMPANY THEN 0 ELSE 1 END
) PM
WHERE
  (@company_id IS NULL OR F.COMPANY = @company_id)
  AND
  ISNULL(F.SODTYPE, 0) = 13
  AND
  (
    (
      ISNULL(F.SOSOURCE, 0) = 1351
      AND ISNULL(F.SOREDIR, 0) IN (0, 10000)
      AND ISNULL(F.TFPRMS, 0) IN (102, 103, 131, 151, 152, 181)
    )
    OR (
      ISNULL(F.SOSOURCE, 0) <> 1351
      AND ISNULL(F.TFPRMS, 0) IN (101, 102, 131, 181)
    )
  )
  -- Retail sales:
  -- 102/103/131 positive, 151/152/181 negative.
  -- Special revenues:
  -- 101/131 positive, 102/181 negative.
  AND (@from_date IS NULL OR F.TRNDATE >= @from_date)
  AND (@to_date IS NULL OR F.TRNDATE < DATEADD(day, 1, @to_date))
  AND (
    @last_sync_ts IS NULL
    OR ISNULL(F.UPDDATE, F.TRNDATE) > @last_sync_ts
    OR (
      ISNULL(F.UPDDATE, F.TRNDATE) = @last_sync_ts
      AND CAST('S|' + CAST(F.FINDOC AS nvarchar(40)) + '|' + CAST(ISNULL(L.MTRLINES, ISNULL(L.LINENUM, 0)) AS nvarchar(40)) AS nvarchar(128)) > CAST(@last_sync_id AS nvarchar(128))
    )
  )
