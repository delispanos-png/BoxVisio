# MENU_STRUCTURE

## Tenant Portal Sidebar Hierarchy

1. `ΚΥΡΙΟ`
   - `Πίνακας` -> `/tenant/dashboard`

2. `ΑΝΑΛΥΤΙΚΑ`
   - `Πωλήσεις` -> `/tenant/sales`
   - `Αγορές` -> `/tenant/purchases`
   - `Στοχεύσεις Προμηθευτών` -> `/tenant/supplier-targets`
   - `Έλεγχος Τιμών` -> `/tenant/price-control`
   - `Απόθεμα` -> `/tenant/inventory`
   - `Είδη Αποθήκης` -> `/tenant/items`
   - `Σύγκριση` -> `/tenant/compare`

3. `Operational Streams / Επιχειρησιακά Κυκλώματα` (grouped parent with icon)
   - `Παραστατικά Πωλήσεων` -> `/tenant/sales-documents`
   - `Παραστατικά Αγορών` -> `/tenant/purchase-documents`
   - `Παραστατικά Αποθήκης` -> `/tenant/warehouse-documents`
   - `Ταμειακές Συναλλαγές` -> `/tenant/cashflow`
   - `Ανοιχτά Υπόλοιπα Προμηθευτών / Υποχρεώσεις` -> `/tenant/suppliers`
   - `Ανοιχτά Υπόλοιπα Πελατών / Απαιτήσεις` -> `/tenant/customers`

4. `ΛΕΙΤΟΥΡΓΙΕΣ`
   - `Insights` -> `/tenant/insights`

## Visibility / Plan Rules

- `Παραστατικά Αποθήκης` uses tenant feature flag `inventory_enabled`.
- `Ταμειακές Συναλλαγές` uses tenant feature flag `cashflow_enabled`.
- Active-state highlighting is enabled at:
  - parent group level (`open` + `active`)
  - submenu item level (`is-active`)

## Admin Portal Review

- The new Operational Streams group is implemented in the **tenant portal** (business execution area).
- The admin portal remains focused on system modules (`Tenants`, `Connections`, `Sync`, `Insights`, `Plans`) and does not expose tenant business-module pages directly to avoid broken/non-contextual routes.
