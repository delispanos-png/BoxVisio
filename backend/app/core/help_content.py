"""Content behind the in-app Help section (/tenant/help).

Three data sets live here:

  CIRCUITS  one entry per screen of the portal — what it is for, how to drive it
            step by step, what the tables and popups hold, what to double-check.
  TASKS     the "how do I find X" index: a user question mapped to the shortest
            route that answers it. This is the entry point people actually use —
            they know their question, not our menu tree.
  FAQ       reconciliation and troubleshooting playbooks.

Everything user-visible is bilingual: `_t(el, en)` for a string, `_tl(el, en)`
for a list. `localize()` flattens a whole entry to one language, and the
`*_for_lang()` helpers are what the routes call.

KPI explanations deliberately do NOT live here. They come from kpi_catalog, so
the manual and the KPI help popups can never disagree.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

#  Screenshots are captured by scripts/capture_manual_screenshots.py into
#  /static/docs/manual/<shot>.jpg. A circuit with shot=None renders without one.
SHOT_BASE = '/static/docs/manual'

#  Keys whose values are language maps produced by _t/_tl. Anything else on an
#  entry (id, route, shot, related, also_kpis) is language-neutral and copied.
_LANG_KEYS = frozenset({'title', 'group', 'purpose', 'steps', 'columns', 'popups', 'checks',
                        'q', 'why', 'tip', 'short'})


def _t(el: str, en: str) -> dict[str, str]:
    return {'el': el, 'en': en}


def _tl(el: list[str], en: list[str]) -> dict[str, list[str]]:
    return {'el': el, 'en': en}


def normalize_lang(lang: str | None) -> str:
    return 'en' if str(lang or '').lower().startswith('en') else 'el'


def localize(value: Any, lang: str = 'el') -> Any:
    """Collapse the {'el': ..., 'en': ...} maps in a structure to one language."""
    lang = normalize_lang(lang)
    if isinstance(value, dict):
        if 'el' in value and 'en' in value and len(value) == 2:
            return value.get(lang) or value.get('el')
        return {k: (localize(v, lang) if k in _LANG_KEYS else v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [localize(v, lang) for v in value]
    return value


def shot_url(shot: str | None) -> str | None:
    return f'{SHOT_BASE}/{shot}.jpg' if shot else None


# --------------------------------------------------------------------------
# Circuits
# --------------------------------------------------------------------------
#  group: which sidebar group the page lives under, so the manual can tell the
#  user where to click rather than only giving them a URL.

CIRCUITS: tuple[dict[str, Any], ...] = (
    {
        'id': 'dashboard',
        'title': _t('Dashboard Διοίκησης', 'Executive Dashboard'),
        'route': '/tenant/dashboard',
        'group': _t('Πίνακες → Executive Dashboard', 'Dashboards → Executive Dashboard'),
        'shot': 'dashboard',
        'purpose': _t(
            'Η πρώτη οθόνη της ημέρας. Δίνει συνολική εικόνα: τζίρος ημέρας/εβδομάδας/μήνα/έτους, '
            'αγορές, μικτό κέρδος, περιθώριο, ποσότητες και σύγκριση με το προηγούμενο έτος.',
            'The screen you start your day on. It gives the whole picture: day / week / month / year '
            'turnover, purchases, gross profit, margin, quantities and a comparison against last year.',
        ),
        'steps': _tl(
            [
                'Άνοιξε το «Φίλτρα» και όρισε περίοδο «Από / Έως». Η ημερομηνία «Έως» είναι η ημέρα αναφοράς για τις κάρτες Ημέρας / Εβδομάδας / Μήνα / YTD.',
                'Διάβασε πρώτα τη σύνοψη επιχειρησιακής εικόνας και τις «Άμεσες ενέργειες» — εκεί συνοψίζεται τι χρειάζεται προσοχή σήμερα.',
                'Πάτα το εικονίδιο σε κάθε κάρτα KPI για να δεις τι ακριβώς υπολογίζει, από ποιο κύκλωμα διαβάζει και τι δεν περιλαμβάνει.',
                'Αν κάποιο νούμερο σε ξενίσει, μπες στο αντίστοιχο κύκλωμα (Πωλήσεις, Αγορές) για να δεις τα παραστατικά που το συνθέτουν.',
            ],
            [
                'Open "Filters" and set the From / To period. The "To" date is the reference day for the Day / Week / Month / YTD cards.',
                'Read the business summary and "Immediate actions" first — that is where today\'s exceptions are collected.',
                'Click the icon on any KPI card to see exactly what it computes, which stream it reads from and what it leaves out.',
                'If a number surprises you, open the matching stream (Sales, Purchases) to see the documents behind it.',
            ],
        ),
        'columns': _tl(
            [
                'Κάρτες KPI για ημέρα, εβδομάδα, μήνα και έτος, με σύγκριση προηγούμενου έτους.',
                'Συνολική εικόνα εταιρείας: πωλήσεις, αγορές, μικτό κέρδος και περιθώριο περιόδου.',
                'Δείκτες υποκαταστημάτων: ενεργά σημεία και συγκέντρωση κορυφαίου καταστήματος.',
            ],
            [
                'KPI cards for day, week, month and year, each with a previous-year comparison.',
                'Company overview: period sales, purchases, gross profit and margin.',
                'Branch indicators: active sites and top-branch concentration.',
            ],
        ),
        'popups': _tl(
            [
                'Κάθε κάρτα KPI ανοίγει επεξήγηση με υπολογισμό, πηγή δεδομένων και παγίδες.',
                'Οι κάρτες με ανάλυση ανοίγουν μηνιαία γραμμή και διάγνωση.',
            ],
            [
                'Every KPI card opens an explanation with the formula, the data source and the pitfalls.',
                'Cards with a breakdown open a monthly series and a diagnosis.',
            ],
        ),
        'checks': _tl(
            [
                'Το «Μικτό Κέρδος Περιόδου» αφαιρεί ΑΓΟΡΕΣ, όχι κόστος πωληθέντων. Σε μήνα με μεγάλη παραγγελία πέφτει χωρίς να έχει χειροτερέψει η κερδοφορία.',
                'Οι κάρτες Ημέρας / Εβδομάδας / Μήνα ακολουθούν την ημερομηνία «Έως», όχι το σημερινό ημερολόγιο.',
                'Πριν βγάλεις συμπέρασμα, έλεγξε την ώρα τελευταίου συγχρονισμού SoftOne πάνω δεξιά.',
            ],
            [
                '"Period Gross Profit" subtracts PURCHASES, not cost of goods sold. In a month with a large order it drops without profitability having worsened.',
                'The Day / Week / Month cards follow the "To" date, not today\'s calendar.',
                'Before drawing a conclusion, check the last SoftOne sync time at the top right.',
            ],
        ),
        'related': ['sales-analytics', 'purchases-analytics', 'finance-dashboard', 'business-advisor'],
    },
    {
        'id': 'finance-dashboard',
        'also_kpis': ['receivables_total', 'receivables_overdue', 'payables_total', 'net_cash', 'aging_buckets'],
        'title': _t('Οικονομικά', 'Finance'),
        'route': '/tenant/finance-dashboard',
        'group': _t('Πίνακες → Οικονομικά', 'Dashboards → Finance'),
        'shot': 'finance-dashboard',
        'purpose': _t(
            'Συγκεντρώνει απαιτήσεις, υποχρεώσεις, εισπράξεις, πληρωμές και δείκτες ρευστότητας σε μία οθόνη.',
            'Brings receivables, payables, collections, payments and liquidity indicators onto one screen.',
        ),
        'steps': _tl(
            [
                'Δες πρώτα το ζεύγος «Συνολικές Απαιτήσεις» και «Υποχρεώσεις Προμηθευτών» — αυτό είναι το καθαρό σου κεφάλαιο κίνησης.',
                'Άνοιξε τις «Ληξιπρόθεσμες» για να δεις πόσο από τις απαιτήσεις έχει ήδη λήξει.',
                'Διάβασε την «Ταμειακή Ροή» της περιόδου μαζί με το μικτό κέρδος: κερδοφορία χωρίς ρευστότητα σημαίνει ότι τα χρήματα είναι δεσμευμένα σε απόθεμα ή σε πελάτες.',
                'Για ονόματα και ποσά ανά πελάτη ή προμηθευτή, πήγαινε στα Υπόλοιπα Πελατών / Προμηθευτών.',
            ],
            [
                'Start with the pair "Total Receivables" and "Supplier Payables" — that is your net working capital.',
                'Open "Overdue" to see how much of what you are owed is already past due.',
                'Read period "Cash Flow" alongside gross profit: profit without liquidity means the money is tied up in stock or in customers.',
                'For names and amounts per customer or supplier, go to Customer / Supplier Balances.',
            ],
        ),
        'columns': _tl(
            [
                'Κάρτες απαιτήσεων, ληξιπρόθεσμων, υποχρεώσεων και ταμειακής ροής.',
                'Πίνακες ανά πελάτη, προμηθευτή ή λογαριασμό.',
                'Aging buckets όπου υπάρχουν ημερομηνίες λήξης.',
            ],
            [
                'Cards for receivables, overdue amounts, payables and cash flow.',
                'Tables by customer, supplier or account.',
                'Aging buckets wherever due dates exist.',
            ],
        ),
        'popups': _tl(
            ['KPI popups με τον ακριβή υπολογισμό κάθε δείκτη.', 'Detail πίνακες για υπόλοιπα και κινήσεις.'],
            ['KPI popups with the exact formula behind each figure.', 'Detail tables for balances and transactions.'],
        ),
        'checks': _tl(
            [
                'Απαιτήσεις και υποχρεώσεις είναι ΤΡΕΧΟΝΤΑ υπόλοιπα: δεν αλλάζουν με την περίοδο «Από / Έως».',
                'Υπόλοιπο χωρίς ημερομηνία λήξης δεν μπορεί να χαρακτηριστεί ληξιπρόθεσμο και δεν μπαίνει στα overdue.',
            ],
            [
                'Receivables and payables are CURRENT balances: the From / To period does not change them.',
                'A balance with no due date cannot be aged and never counts as overdue.',
            ],
        ),
        'related': ['customer-balances', 'supplier-balances', 'cash-transactions'],
    },
    {
        'id': 'insights',
        'title': _t('Insights / Προειδοποιήσεις', 'Insights / Alerts'),
        'route': '/tenant/insights',
        'group': _t('Πίνακες → Insights', 'Dashboards → Insights'),
        'shot': 'insights',
        'purpose': _t(
            'Αυτόματες επιχειρησιακές προειδοποιήσεις για πωλήσεις, αγορές, αποθέματα, απαιτήσεις, '
            'υποχρεώσεις και εμπορικό κίνδυνο. Το BI σε ειδοποιεί αντί να ψάχνεις εσύ.',
            'Automatic business alerts across sales, purchases, stock, receivables, payables and '
            'commercial risk. The BI tells you, instead of you having to go looking.',
        ),
        'steps': _tl(
            [
                'Ξεκίνα από τα insights με σοβαρότητα «high» — είναι ταξινομημένα ώστε το κρίσιμο να είναι πρώτο.',
                'Πάτα το drill-down κάθε insight για να δεις τα δεδομένα πάνω στα οποία βασίστηκε.',
                'Αν το θέμα είναι πραγματικό, ενέργησε στο αντίστοιχο κύκλωμα. Αν όχι, σημείωσέ το ως χειρισμένο.',
            ],
            [
                'Start with the "high" severity insights — they are ordered so the critical ones come first.',
                'Use each insight\'s drill-down to see the data it was based on.',
                'If the issue is real, act in the relevant stream. If not, mark it as handled.',
            ],
        ),
        'columns': _tl(
            [
                'Ημερομηνία, κατηγορία, σοβαρότητα, τίτλος και περιγραφή.',
                'Σύνδεσμος drill-down προς τη σχετική σελίδα.',
                'Κατάσταση χειρισμού όπου υποστηρίζεται.',
            ],
            [
                'Date, category, severity, title and description.',
                'A drill-down link to the relevant page.',
                'Handling status where supported.',
            ],
        ),
        'popups': _tl(
            ['Detail insight με εξήγηση και προτεινόμενη ενέργεια.', 'Drill-down προς πωλήσεις, αγορές, απόθεμα ή οικονομικά.'],
            ['Insight detail with an explanation and a suggested action.', 'Drill-down into sales, purchases, stock or finance.'],
        ),
        'checks': _tl(
            [
                'Το insight είναι ΕΝΔΕΙΞΗ προς έλεγχο, όχι λογιστικό αποτέλεσμα.',
                'Άνοιξε πάντα το drill-down πριν κλείσεις ένα θέμα.',
            ],
            [
                'An insight is a SIGNAL to check, not an accounting result.',
                'Always open the drill-down before closing an item.',
            ],
        ),
        'related': ['business-advisor', 'dashboard'],
    },
    {
        'id': 'business-advisor',
        'also_kpis': ['margin_pct', 'days_of_supply', 'gross_profit', 'net_cash'],
        'title': _t('Σύμβουλος Επιχείρησης', 'Business Advisor'),
        'route': '/tenant/business-advisor',
        'group': _t('Αναλύσεις → Business Advisor', 'Analytics → Business Advisor'),
        'shot': 'business-advisor',
        'purpose': _t(
            'Συμπυκνωμένη επιχειρησιακή διάγνωση για ιδιοκτήτη ή διοίκηση. Συνδυάζει πωλήσεις, margin, '
            'απόθεμα, τιμές, φυσικά σημεία και κατηγορίες, και προτείνει συγκεκριμένες ενέργειες.',
            'A condensed business diagnosis for the owner or management. It combines sales, margin, '
            'stock, pricing, physical sites and categories, and proposes concrete actions.',
        ),
        'steps': _tl(
            [
                'Δες το συνολικό score και τους δύο-τρεις «μοχλούς βελτίωσης» με τη μεγαλύτερη επίδραση.',
                'Άνοιξε κάθε μοχλό: εξηγεί γιατί εμφανίστηκε και σε στέλνει στο κύκλωμα που τον λύνει.',
                'Έλεγξε τη διάγνωση φυσικών σημείων για να δεις ποιο σημείο τραβάει την εικόνα πάνω ή κάτω.',
            ],
            [
                'Look at the overall score and the two or three highest-impact "improvement levers".',
                'Open each lever: it explains why it appeared and links to the stream that resolves it.',
                'Check the site diagnosis to see which location is pulling the picture up or down.',
            ],
        ),
        'columns': _tl(
            [
                'Σύνοψη διοίκησης με score και βασικούς δείκτες.',
                'Μοχλοί βελτίωσης με προτεινόμενη κίνηση.',
                'Διάγνωση φυσικών σημείων και κατηγοριών.',
            ],
            [
                'Management summary with a score and headline indicators.',
                'Improvement levers with a suggested move.',
                'Diagnosis by physical site and by category.',
            ],
        ),
        'popups': _tl(
            ['Άνοιγμα ανάλυσης από κάθε μοχλό βελτίωσης.', 'Σύνδεσμος προς το σχετικό κύκλωμα.'],
            ['Each improvement lever opens its analysis.', 'A link through to the relevant stream.'],
        ),
        'checks': _tl(
            [
                'Είναι διοικητική διάγνωση, όχι λογιστική κατάσταση.',
                'Οι στόχοι margin και ημερών κάλυψης είναι παραμετρικοί ανά tenant — αν είναι μη ρεαλιστικοί, το score παραπλανά.',
            ],
            [
                'This is a management diagnosis, not a financial statement.',
                'Margin and coverage targets are configured per tenant — if they are unrealistic, the score misleads.',
            ],
        ),
        'related': ['insights', 'price-control', 'sellout'],
    },

    # ---------------- Επιχειρησιακά κυκλώματα ----------------------------
    {
        'id': 'sales-documents',
        'title': _t('Παραστατικά Πωλήσεων', 'Sales Documents'),
        'route': '/tenant/sales-documents',
        'group': _t('Επιχειρησιακά Κυκλώματα → Παραστατικά Πωλήσεων', 'Operational Streams → Sales Documents'),
        'shot': 'sales-documents',
        'purpose': _t(
            'Η αναλυτική λίστα παραστατικών πωλήσεων με γραμμές, καθαρή αξία, έξοδα, ΦΠΑ και γενικό σύνολο. '
            'Εδώ έρχεσαι όταν θέλεις να δεις ΤΟ ΠΑΡΑΣΤΑΤΙΚΟ, όχι τη σύνοψη.',
            'The itemised list of sales documents with lines, net value, charges, VAT and grand total. '
            'This is where you come for THE DOCUMENT, not the summary.',
        ),
        'steps': _tl(
            [
                'Όρισε περίοδο και, αν χρειάζεται, υποκατάστημα, αποθηκευτικό χώρο, κανάλι ή σειρά.',
                'Χρησιμοποίησε την αναζήτηση για συγκεκριμένο παραστατικό, πελάτη ή κωδικό.',
                'Διπλό κλικ σε γραμμή ανοίγει την καρτέλα του παραστατικού με τις γραμμές ειδών.',
                'Διάβασε το footer: Καθαρή Αξία, Έξοδα, ΦΠΑ και Σύνολο είναι ξεχωριστά μεγέθη — αυτά συγκρίνεις με το SoftOne.',
            ],
            [
                'Set the period and, if needed, branch, warehouse, channel or series.',
                'Use search for a specific document, customer or code.',
                'Double-click a row to open the document with its item lines.',
                'Read the footer: Net Value, Charges, VAT and Total are separate figures — those are what you reconcile against SoftOne.',
            ],
        ),
        'columns': _tl(
            [
                'Ημερομηνία, κατάστημα, σειρά, κανάλι, πελάτης, παράδοση.',
                'Καθαρή αξία, έξοδα, ΦΠΑ, σύνολο.',
                'Στο detail φαίνονται γραμμές ειδών και στοιχεία παραστατικού.',
            ],
            [
                'Date, branch, series, channel, customer, delivery.',
                'Net value, charges, VAT, total.',
                'The detail view shows item lines and document data.',
            ],
        ),
        'popups': _tl(
            ['Άνοιγμα παραστατικού με εικονίδιο ή διπλό κλικ.', 'Popup γραμμών για είδος, ποσότητα, τιμή, έκπτωση και ΦΠΑ.'],
            ['Open a document via the icon or a double-click.', 'A lines popup for item, quantity, price, discount and VAT.'],
        ),
        'checks': _tl(
            [
                'Αν συμφωνεί το πλήθος παραστατικών αλλά όχι οι αξίες, η διαφορά είναι στα έξοδα ή στον ΦΠΑ.',
                'Τα έξοδα παραστατικού δεν είναι πωλούμενο είδος — δεν έχουν ποσότητα και δεν μπαίνουν στην ανάλυση ειδών.',
                'Έλεγξε κανάλι/σειρά όταν ψάχνεις να ξεχωρίσεις eShop από φυσικό κατάστημα.',
            ],
            [
                'If the document count matches but the values do not, the difference is in charges or VAT.',
                'Document charges are not a sold item — they carry no quantity and never enter item analysis.',
                'Check channel/series when separating e-shop from the physical store.',
            ],
        ),
        'related': ['sales-analytics', 'pos', 'eshop-analysis'],
    },
    {
        'id': 'purchase-documents',
        'also_kpis': ['doc_count', 'doc_net_value', 'doc_expenses', 'doc_vat', 'doc_total'],
        'title': _t('Παραστατικά Αγορών', 'Purchase Documents'),
        'route': '/tenant/purchase-documents',
        'group': _t('Επιχειρησιακά Κυκλώματα → Παραστατικά Αγορών', 'Operational Streams → Purchase Documents'),
        'shot': 'purchase-documents',
        'purpose': _t(
            'Αναλυτική εικόνα αγορών με καθαρή αξία, έξοδα, ΦΠΑ και σύνολο, ανά προμηθευτή και παραστατικό.',
            'The itemised purchase picture with net value, charges, VAT and total, by supplier and document.',
        ),
        'steps': _tl(
            [
                'Όρισε περίοδο και προμηθευτή ή αποθήκη αν ψάχνεις κάτι συγκεκριμένο.',
                'Άνοιξε το παραστατικό για να δεις γραμμές, ποσότητες, κόστος και εκπτώσεις.',
                'Για τη συνολική εικόνα δαπάνης ανά προμηθευτή, πήγαινε στην Ανάλυση Αγορών.',
            ],
            [
                'Set the period, plus supplier or warehouse if you are after something specific.',
                'Open a document to see lines, quantities, cost and discounts.',
                'For total spend per supplier, go to Purchases Analytics.',
            ],
        ),
        'columns': _tl(
            ['Προμηθευτής, σειρά, τύπος, παραστατικό, αποθήκη.', 'Καθαρή αξία, έξοδα, ΦΠΑ, σύνολο.'],
            ['Supplier, series, type, document, warehouse.', 'Net value, charges, VAT, total.'],
        ),
        'popups': _tl(
            ['Άνοιγμα παραστατικού για γενικά στοιχεία και γραμμές.', 'Τα πιστωτικά εμφανίζονται με αρνητική επίδραση όπου το ορίζει ο κανόνας.'],
            ['Open a document for its header data and lines.', 'Credit notes show a negative effect wherever the rule defines it.'],
        ),
        'checks': _tl(
            [
                'Συμμετέχουν μόνο οι συμπεριφορές παραστατικών που έχει ορίσει ο tenant ως αγορές.',
                'Απόφυγε σύγκριση με παραστατικά που δεν ανήκουν στον κύκλο αγορών (π.χ. έξοδα).',
            ],
            [
                'Only the document behaviours the tenant marks as purchases participate.',
                'Avoid comparing against documents outside the purchase cycle (expenses, for instance).',
            ],
        ),
        'related': ['purchases-analytics', 'supplier-orders', 'supplier-balances'],
    },
    {
        'id': 'supplier-orders',
        'title': _t('Παραγγελίες Προμηθευτών', 'Supplier Orders'),
        'route': '/tenant/supplier-orders',
        'group': _t('Επιχειρησιακά Κυκλώματα → Παραγγελίες Προμηθευτών', 'Operational Streams → Supplier Orders'),
        'shot': 'supplier-orders',
        'purpose': _t(
            'Έλεγχος ανοιχτών παραγγελιών προς προμηθευτές — τι έχεις ήδη παραγγείλει πριν παραγγείλεις ξανά.',
            'A check on open supplier orders — what you have already ordered, before you order again.',
        ),
        'steps': _tl(
            [
                'Άφησε το φίλτρο σε «Μόνο ανοιχτές» για να δεις τι εκκρεμεί.',
                'Φίλτραρε ανά προμηθευτή όταν ετοιμάζεις νέα παραγγελία.',
                'Πριν αποδεχτείς πρόταση στο FnR ή στο Replenishment, πέρνα πρώτα από εδώ.',
            ],
            [
                'Leave the filter on "Open only" to see what is outstanding.',
                'Filter by supplier when preparing a new order.',
                'Before accepting a suggestion in FnR or Replenishment, come through here first.',
            ],
        ),
        'columns': _tl(
            [
                'Ημερομηνία, παραστατικό, σειρά και προμηθευτής.',
                'Ποσότητα παραγγελίας, καλυμμένη ποσότητα, ακυρωμένη ποσότητα και αξία γραμμής.',
                'Κατάσταση Ανοιχτή / Κλειστή.',
            ],
            [
                'Date, document, series and supplier.',
                'Ordered quantity, covered quantity, cancelled quantity and line value.',
                'Open / Closed status.',
            ],
        ),
        'popups': _tl(
            ['Γραμμές ειδών για έλεγχο πριν το replenishment.', 'Φίλτρο προμηθευτή/παραστατικού και επιλογή «Μόνο ανοιχτές» ή «Όλες».'],
            ['Item lines to review before replenishment.', 'A supplier/document filter and an "Open only" or "All" toggle.'],
        ),
        'checks': _tl(
            [
                'Ανοιχτή είναι μόνο η παραγγελία χωρίς μετασχηματισμό. Μερική παράδοση τη θεωρεί κλειστή.',
                'Το BI δεν κρατά backorders.',
                'Το πόσες ημέρες πίσω κοιτάει η σελίδα ορίζεται στις Ρυθμίσεις tenant (lookback).',
            ],
            [
                'Only an untransformed order counts as open. A partial delivery closes it.',
                'BI does not track backorders.',
                'How many days back the page looks is set in tenant Settings (lookback).',
            ],
        ),
        'related': ['fnr', 'replenishment', 'purchase-documents'],
    },
    {
        'id': 'warehouse-documents',
        'also_kpis': ['doc_count', 'doc_net_value', 'doc_total'],
        'title': _t('Παραστατικά Αποθήκης', 'Warehouse Documents'),
        'route': '/tenant/warehouse-documents',
        'group': _t('Επιχειρησιακά Κυκλώματα → Παραστατικά Αποθήκης', 'Operational Streams → Warehouse Documents'),
        'shot': 'warehouse-documents',
        'purpose': _t(
            'Κινήσεις αποθήκης: μεταφορές μεταξύ σημείων, εισαγωγές, εξαγωγές και ό,τι μετακινεί ποσότητες.',
            'Warehouse movements: transfers between sites, receipts, issues and anything that moves quantity.',
        ),
        'steps': _tl(
            [
                'Φίλτραρε ανά αποθήκη όταν ψάχνεις να εξηγήσεις διαφορά αποθέματος.',
                'Ξεχώρισε τις μεταφορές (από αποθήκη → σε αποθήκη) από τις καθαρές εισαγωγές/εξαγωγές.',
                'Άνοιξε το παραστατικό για να δεις τις γραμμές ειδών που κινήθηκαν.',
            ],
            [
                'Filter by warehouse when explaining a stock discrepancy.',
                'Separate transfers (warehouse → warehouse) from genuine receipts and issues.',
                'Open a document to see which item lines moved.',
            ],
        ),
        'columns': _tl(
            ['Ημερομηνία, αποθήκη από/σε, σειρά, παραστατικό, κατάσταση.', 'Ποσότητες και αξία κίνησης.'],
            ['Date, from/to warehouse, series, document, status.', 'Movement quantities and value.'],
        ),
        'popups': _tl(
            ['Καρτέλα παραστατικού αποθήκης.', 'Ανάλυση γραμμών για είδος και ποσότητα.'],
            ['The warehouse document card.', 'A line breakdown by item and quantity.'],
        ),
        'checks': _tl(
            [
                'Η μεταφορά δεν αλλάζει το συνολικό απόθεμα της εταιρείας, μόνο την κατανομή του ανά σημείο.',
                'Όταν δεν βγαίνει το απόθεμα ενός σημείου, ξεκίνα από εδώ πριν κοιτάξεις πωλήσεις.',
            ],
            [
                'A transfer does not change company-wide stock, only how it is distributed across sites.',
                'When one site\'s stock does not add up, start here before looking at sales.',
            ],
        ),
        'related': ['inventory-analytics', 'warehouse-items'],
    },
    {
        'id': 'expense-documents',
        'also_kpis': ['doc_count', 'doc_net_value', 'doc_expenses', 'doc_vat', 'doc_total'],
        'title': _t('Παραστατικά Εξόδων', 'Expense Documents'),
        'route': '/tenant/expense-documents',
        'group': _t('Επιχειρησιακά Κυκλώματα → Παραστατικά Εξόδων', 'Operational Streams → Expense Documents'),
        'shot': 'expense-documents',
        'purpose': _t(
            'Παραστατικά δαπανών και εξόδων, μαζί με ειδικές κατηγορίες προμηθευτών/πιστωτών όπου έχουν οριστεί.',
            'Expense and cost documents, including the special supplier/creditor categories where configured.',
        ),
        'steps': _tl(
            [
                'Όρισε περίοδο και κατηγορία δαπάνης.',
                'Άνοιξε το παραστατικό για να δεις την ανάλυση και τη σειρά.',
                'Για συγκεντρωτική εικόνα ανά κατηγορία, πήγαινε στα Λειτουργικά Έξοδα.',
            ],
            [
                'Set the period and the expense category.',
                'Open a document to see its breakdown and series.',
                'For the aggregate picture by category, go to Operating Expenses.',
            ],
        ),
        'columns': _tl(
            ['Ημερομηνία, κατηγορία εξόδου, υποκατάστημα, σειρά, προμηθευτής/πιστωτής.', 'Καθαρή αξία, έξοδα, ΦΠΑ, σύνολο.'],
            ['Date, expense category, branch, series, supplier/creditor.', 'Net value, charges, VAT, total.'],
        ),
        'popups': _tl(
            ['Άνοιγμα παραστατικού εξόδων.', 'Έλεγχος κατηγορίας δαπάνης και σειράς.'],
            ['Open an expense document.', 'Check the expense category and series.'],
        ),
        'checks': _tl(
            [
                'Ξεχώρισε πάντα τα έξοδα από τις αγορές εμπορευμάτων — είναι διαφορετικά κυκλώματα και δεν αθροίζονται.',
                'Επιβεβαίωσε ότι οι ειδικές προμηθευτών/πιστωτών έχουν σωστή συμπεριφορά.',
            ],
            [
                'Always keep expenses separate from merchandise purchases — different streams, they do not add up.',
                'Confirm that special supplier/creditor documents carry the right behaviour.',
            ],
        ),
        'related': ['operating-expenses', 'cash-transactions'],
    },
    {
        'id': 'operating-expenses',
        'title': _t('Λειτουργικά Έξοδα', 'Operating Expenses'),
        'route': '/tenant/operating-expenses',
        'group': _t('Επιχειρησιακά Κυκλώματα → Λειτουργικά Έξοδα', 'Operational Streams → Operating Expenses'),
        'shot': 'operating-expenses',
        'purpose': _t(
            'Συγκεντρωτική εικόνα λειτουργικών δαπανών ανά κατηγορία, με δείκτη βαρύτητας πάνω στον τζίρο.',
            'The aggregate view of operating spend by category, with its weight against turnover.',
        ),
        'steps': _tl(
            [
                'Όρισε περίοδο — ιδανικά ολόκληρους μήνες, ώστε η σύγκριση να έχει νόημα.',
                'Δες το «Έξοδα / Έσοδα» για να καταλάβεις πόσο βαραίνουν σε σχέση με τον τζίρο.',
                'Ψάξε κατηγορίες με ασυνήθιστη αύξηση μήνα με μήνα.',
            ],
            [
                'Set the period — ideally whole months, so the comparison means something.',
                'Read "Expenses / Revenue" to see how heavy they are against turnover.',
                'Look for categories with an unusual month-on-month increase.',
            ],
        ),
        'columns': _tl(
            ['Κατηγορία, ημερομηνία, προμηθευτής/αιτιολογία, ποσό.', 'Σύνολα ανά κατηγορία.'],
            ['Category, date, supplier/description, amount.', 'Totals by category.'],
        ),
        'popups': _tl(
            ['KPI popups με τον υπολογισμό κάθε δείκτη.', 'Detail δαπάνης όπου υπάρχει.'],
            ['KPI popups with each figure\'s formula.', 'Expense detail where available.'],
        ),
        'checks': _tl(
            [
                'Το «Έξοδα / Έσοδα» συγκρίνει με τζίρο, όχι με μικτό κέρδος. Για να δεις αν μένει αποτέλεσμα, σύγκρινε τα έξοδα με το μικτό κέρδος της ίδιας περιόδου.',
                'Πολλές κατηγορίες με μικρά ποσά συνήθως σημαίνει ασυνεπή κωδικοποίηση στο SoftOne.',
            ],
            [
                '"Expenses / Revenue" compares against turnover, not gross profit. To see what is left, compare expenses against the same period\'s gross profit.',
                'Many categories holding tiny amounts usually means inconsistent coding in SoftOne.',
            ],
        ),
        'related': ['expense-documents', 'cash-transactions', 'finance-dashboard'],
    },
    {
        'id': 'cash-transactions',
        'title': _t('Ταμειακές Ροές', 'Cash Flow'),
        'route': '/tenant/cashflow',
        'group': _t('Επιχειρησιακά Κυκλώματα → Ταμειακές Ροές', 'Operational Streams → Cash Flow'),
        'shot': 'cash-transactions',
        'purpose': _t(
            'Εισπράξεις, πληρωμές, μεταφορές μεταξύ λογαριασμών και καθαρό αποτέλεσμα ρευστότητας.',
            'Collections, payments, transfers between accounts and the net liquidity result.',
        ),
        'steps': _tl(
            [
                'Διάλεξε κατηγορία από το μενού (εισπράξεις πελατών, πληρωμές προμηθευτών, μεταφορές, λογαριασμοί).',
                'Όρισε περίοδο και λογαριασμό.',
                'Διάβασε την Καθαρή Ροή μαζί με τις εισροές και εκροές — το πρόσημο μόνο του δεν λέει την ιστορία.',
            ],
            [
                'Pick a category from the menu (customer collections, supplier payments, transfers, accounts).',
                'Set the period and the account.',
                'Read Net Flow together with inflows and outflows — the sign alone does not tell the story.',
            ],
        ),
        'columns': _tl(
            ['Ημερομηνία, λογαριασμός, τύπος κίνησης, αντισυμβαλλόμενος.', 'Ποσό εισροής / εκροής και κατηγορία.'],
            ['Date, account, transaction type, counterparty.', 'Inflow / outflow amount and category.'],
        ),
        'popups': _tl(
            ['Detail κίνησης.', 'Σύνολα στο footer και εξαγωγές ανά κατηγορία.'],
            ['Transaction detail.', 'Footer totals and exports per category.'],
        ),
        'checks': _tl(
            [
                'Οι μεταφορές μεταξύ λογαριασμών ΔΕΝ είναι πραγματική δαπάνη — φίλτραρε ανά κατηγορία πριν κρίνεις ρευστότητα.',
                'Ταμειακή ροή ≠ τζίρος: η πώληση επί πιστώσει γράφεται σήμερα και εισπράττεται αργότερα.',
            ],
            [
                'Transfers between accounts are NOT real spend — filter by category before judging liquidity.',
                'Cash flow ≠ turnover: a credit sale is booked today and collected later.',
            ],
        ),
        'related': ['finance-dashboard', 'customer-balances', 'supplier-balances'],
    },
    {
        'id': 'supplier-balances',
        'also_kpis': ['aging_buckets'],
        'title': _t('Υπόλοιπα Προμηθευτών', 'Supplier Balances'),
        'route': '/tenant/suppliers',
        'group': _t('Επιχειρησιακά Κυκλώματα → Υπόλοιπα Προμηθευτών', 'Operational Streams → Supplier Balances'),
        'shot': 'supplier-balances',
        'purpose': _t(
            'Ανοικτές υποχρεώσεις ανά προμηθευτή, ληξιπρόθεσμα και ενηλικίωση υπολοίπων.',
            'Open payables by supplier, overdue amounts and balance aging.',
        ),
        'steps': _tl(
            [
                'Ταξινόμησε κατά υπόλοιπο για να δεις πού είναι συγκεντρωμένη η έκθεσή σου.',
                'Δώσε προτεραιότητα στα ληξιπρόθεσμα.',
                'Σύγκρινε με τις αγορές της περιόδου: μεγάλο υπόλοιπο με μικρές αγορές σημαίνει καθυστερημένες πληρωμές.',
            ],
            [
                'Sort by balance to see where your exposure is concentrated.',
                'Prioritise the overdue amounts.',
                'Compare against period purchases: a large balance with small purchases means late payments.',
            ],
        ),
        'columns': _tl(
            ['Κωδικός, επωνυμία, υπόλοιπο, ληξιπρόθεσμο, aging.', 'Σύνολα στο footer και αναζήτηση προμηθευτή.'],
            ['Code, name, balance, overdue, aging.', 'Footer totals and supplier search.'],
        ),
        'popups': _tl(
            ['Detail προμηθευτή και ιστορικό όπου υπάρχει.'],
            ['Supplier detail and history where available.'],
        ),
        'checks': _tl(
            ['Είναι τρέχον υπόλοιπο — δεν επηρεάζεται από την περίοδο.', 'Τα πιστωτικά υπόλοιπα προμηθευτή μειώνουν το σύνολο.'],
            ['This is a current balance — the period does not affect it.', 'Supplier credit balances reduce the total.'],
        ),
        'related': ['purchase-documents', 'finance-dashboard', 'cash-transactions'],
    },
    {
        'id': 'customer-balances',
        'title': _t('Υπόλοιπα Πελατών', 'Customer Balances'),
        'route': '/tenant/customers',
        'group': _t('Επιχειρησιακά Κυκλώματα → Υπόλοιπα Πελατών', 'Operational Streams → Customer Balances'),
        'shot': 'customer-balances',
        'purpose': _t(
            'Απαιτήσεις ανά πελάτη, ληξιπρόθεσμα, aging και ιστορική εικόνα.',
            'Receivables by customer, overdue amounts, aging and history.',
        ),
        'steps': _tl(
            [
                'Ταξινόμησε κατά ανοικτό υπόλοιπο και δες αν λίγοι πελάτες κρατούν μεγάλο μέρος.',
                'Εστίασε στα ληξιπρόθεσμα και στις παλαιότερες ζώνες aging.',
                'Διπλό κλικ σε γραμμή ανοίγει την καρτέλα του πελάτη.',
            ],
            [
                'Sort by open balance and check whether a few customers hold most of it.',
                'Focus on the overdue amounts and the oldest aging bands.',
                'Double-click a row to open the customer card.',
            ],
        ),
        'columns': _tl(
            ['Κωδικός, επωνυμία, υπόλοιπο, ληξιπρόθεσμο, aging.', 'Σύνολα στο footer και αναζήτηση πελάτη.'],
            ['Code, name, balance, overdue, aging.', 'Footer totals and customer search.'],
        ),
        'popups': _tl(
            ['Detail πελάτη και ιστορικό κινήσεων όπου υπάρχει.'],
            ['Customer detail and transaction history where available.'],
        ),
        'checks': _tl(
            [
                'Είναι τρέχον υπόλοιπο — δεν επηρεάζεται από την περίοδο.',
                'Υπόλοιπο χωρίς ημερομηνία λήξης δεν ενηλικιώνεται και δεν εμφανίζεται ως ληξιπρόθεσμο.',
            ],
            [
                'This is a current balance — the period does not affect it.',
                'A balance with no due date cannot be aged and never shows as overdue.',
            ],
        ),
        'related': ['finance-dashboard', 'cash-transactions', 'sales-documents'],
    },

    # ---------------- Αναλύσεις -------------------------------------------
    {
        'id': 'sales-analytics',
        'title': _t('Ανάλυση Πωλήσεων', 'Sales Analytics'),
        'route': '/tenant/sales',
        'group': _t('Αναλύσεις → Πωλήσεις', 'Analytics → Sales'),
        'shot': 'sales-analytics',
        'purpose': _t(
            'Αναλύει τζίρο, ποσότητα, μικτό κέρδος και περιθώριο ανά είδος, κατηγορία, brand, πελάτη και υποκατάστημα.',
            'Breaks turnover, quantity, gross profit and margin down by item, category, brand, customer and branch.',
        ),
        'steps': _tl(
            [
                'Όρισε περίοδο και, αν χρειάζεται, υποκατάστημα ή κανάλι.',
                'Δες πρώτα αν η μεταβολή του τζίρου προήλθε από ΠΟΣΟΤΗΤΑ ή από ΤΙΜΗ: σύγκρινε «Συνολικός Τζίρος» με «Πωληθείσα Ποσότητα».',
                'Κατέβα στα breakdowns για να βρεις ποια κατηγορία ή ποιο είδος οδηγεί τη μεταβολή.',
                'Χρησιμοποίησε τη «Μεταβολή vs Προηγούμενη» για να δεις αν η τάση είναι πραγματική ή εποχική.',
            ],
            [
                'Set the period and, if needed, branch or channel.',
                'First establish whether a turnover change came from QUANTITY or from PRICE: compare "Total Turnover" against "Units Sold".',
                'Drill into the breakdowns to find which category or item is driving the change.',
                'Use "Growth vs Previous" to tell a real trend from a seasonal one.',
            ],
        ),
        'columns': _tl(
            [
                'Breakdown ανά υποκατάστημα, κατηγορία, brand και είδος.',
                'Trends ανά ημέρα / μήνα.',
                'Πίνακες κορυφαίων και χαμηλότερων ειδών.',
            ],
            [
                'Breakdowns by branch, category, brand and item.',
                'Daily / monthly trends.',
                'Top and bottom item tables.',
            ],
        ),
        'popups': _tl(
            ['KPI popups με ακριβή υπολογισμό.', 'Charts και drill-down πίνακες, με εξαγωγή όπου διατίθεται.'],
            ['KPI popups with the exact formula.', 'Charts and drill-down tables, with export where available.'],
        ),
        'checks': _tl(
            [
                'Το «Μικτό Κέρδος» εδώ αφαιρεί ΚΟΣΤΟΣ ΠΩΛΗΘΕΝΤΩΝ — είναι το σωστό μέτρο κερδοφορίας, σε αντίθεση με το «Μικτό Κέρδος Περιόδου» του Dashboard.',
                'Αν λείπει κόστος από γραμμές, το περιθώριο βγαίνει τεχνητά υψηλό.',
                'Δες αν λίγα είδη κρατούν δυσανάλογο μέρος του τζίρου.',
            ],
            [
                '"Gross Profit" here subtracts COST OF GOODS SOLD — the correct profitability measure, unlike the Dashboard\'s "Period Gross Profit".',
                'If lines are missing cost, the margin reads artificially high.',
                'Check whether a few items hold a disproportionate share of turnover.',
            ],
        ),
        'related': ['sales-documents', 'pos', 'eshop-analysis', 'sellout'],
    },
    {
        'id': 'pos',
        'also_kpis': ['sales_period'],
        'title': _t('Φυσικό Σημείο / POS', 'Point of Sale'),
        'route': '/tenant/pos',
        'group': _t('Αναλύσεις → POS', 'Analytics → POS'),
        'shot': 'pos',
        'purpose': _t(
            'Εστιάζει στη λιανική φυσικού σημείου: αποδείξεις, μέση αξία καλαθιού, είδη ανά απόδειξη και τρόποι πληρωμής.',
            'Focused on in-store retail: receipts, average basket value, items per receipt and payment methods.',
        ),
        'steps': _tl(
            [
                'Δες τη «Μέση Αξία Απόδειξης» και τον «Μέσο Αριθμό Ειδών» μαζί — αυτά τα δύο εξηγούν σχεδόν κάθε μεταβολή λιανικής.',
                'Αν πέφτει ο τζίρος, έλεγξε πρώτα αν έπεσε το πλήθος αποδείξεων (επισκεψιμότητα) ή η μέση αξία (καλάθι).',
                'Σύγκρινε φυσικά σημεία μεταξύ τους με το φίλτρο υποκαταστήματος.',
            ],
            [
                'Read "Average Receipt Value" and "Average Items per Receipt" together — those two explain nearly every retail movement.',
                'If turnover is falling, first check whether it is receipt count (footfall) or average value (basket) that dropped.',
                'Compare sites against each other using the branch filter.',
            ],
        ),
        'columns': _tl(
            ['Φίλτρα ανά υποκατάστημα και περίοδο.', 'Κατανομή τρόπων πληρωμής και κατηγοριών.', 'Γραφήματα τάσης.'],
            ['Filters by branch and period.', 'Payment-method and category distribution.', 'Trend charts.'],
        ),
        'popups': _tl(
            ['Popup τρόπων πληρωμής.', 'Popup κατηγοριών / ομάδων ειδών.'],
            ['A payment-methods popup.', 'A popup for item categories / groups.'],
        ),
        'checks': _tl(
            [
                'Οι αποδείξεις μετράνε παραστατικά, όχι μοναδικούς πελάτες.',
                'Άνοδος στα είδη ανά απόδειξη με σταθερή μέση αξία σημαίνει ότι πουλάς περισσότερα φθηνά είδη.',
            ],
            [
                'Receipts count documents, not unique customers.',
                'More items per receipt at a flat average value means you are selling more cheap items.',
            ],
        ),
        'related': ['sales-analytics', 'sales-documents'],
    },
    {
        'id': 'call-center',
        'title': _t('Τηλεφωνικό Κέντρο', 'Call Center'),
        'route': '/tenant/call-center',
        'group': _t('Αναλύσεις → Call Center', 'Analytics → Call Center'),
        'shot': 'call-center',
        'purpose': _t(
            'Κίνηση κλήσεων, χρόνοι αναμονής και ομιλίας, εισερχόμενες και εξερχόμενες, ανά περίοδο και χειριστή.',
            'Call traffic, wait and talk times, inbound and outbound, by period and agent.',
        ),
        'steps': _tl(
            [
                'Όρισε περίοδο και δες πρώτα τη συνολική κίνηση κλήσεων.',
                'Έλεγξε τους μέσους χρόνους αναμονής και ομιλίας ανά ζώνη ώρας για να βρεις πού χάνεις κλήσεις.',
                'Κατέβασε την αναφορά σε Excel για ανάλυση ανά χειριστή.',
            ],
            [
                'Set the period and start with total call traffic.',
                'Check average wait and talk times by time band to find where you lose calls.',
                'Export to Excel for per-agent analysis.',
            ],
        ),
        'columns': _tl(
            ['Κλήσεις ανά ημέρα και ζώνη ώρας.', 'Μέσος χρόνος αναμονής και ομιλίας.', 'Εισερχόμενες, εξερχόμενες και αναπάντητες.'],
            ['Calls by day and time band.', 'Average wait and talk time.', 'Inbound, outbound and missed.'],
        ),
        'popups': _tl(
            ['Εξαγωγές Excel ανά αναφορά (κίνηση, εισερχόμενες, εξερχόμενες).'],
            ['Excel exports per report (traffic, inbound, outbound).'],
        ),
        'checks': _tl(
            [
                'Οι μέσοι χρόνοι υπολογίζονται ανά περίοδο από τις ίδιες τις κλήσεις, όχι από αποθηκευμένα αθροίσματα.',
                'Χρειάζεται ενεργή σύνδεση με το τηλεφωνικό κέντρο — αν λείπουν ημέρες, έλεγξε τη σύνδεση.',
            ],
            [
                'Averages are recomputed per period from the call rows themselves, not from stored sums.',
                'It needs a live phone-system connection — if days are missing, check that connection.',
            ],
        ),
        'related': ['eshop-analysis'],
    },
    {
        'id': 'eshop-analysis',
        'title': _t('Ανάλυση e-Shop', 'E-Shop Analytics'),
        'route': '/tenant/e-shop-analysis',
        'group': _t('Αναλύσεις → e-Shop', 'Analytics → E-Shop'),
        'shot': 'eshop-analysis',
        'purpose': _t(
            'Αναλύει το ηλεκτρονικό κανάλι: τζίρο, παραγγελίες, μέση παραγγελία, μεταφορικά, αντικαταβολές, '
            'πόλεις παράδοσης και μοντέλο εκτέλεσης.',
            'Analyses the online channel: turnover, orders, average order value, shipping, cash-on-delivery, '
            'delivery cities and the fulfilment model.',
        ),
        'steps': _tl(
            [
                'Δες τη «Μέση Παραγγελία» δίπλα στο «Κόστος courier / έσοδα» — αυτό το ζευγάρι λέει αν το κανάλι βγάζει κέρδος.',
                'Έλεγξε τις «Παραγγελίες χωρίς μεταφορική»: μεγάλο νούμερο σημαίνει ότι υποεκτιμάς το κόστος αποστολής.',
                'Χρησιμοποίησε τα breakdowns ανά μεταφορική και πόλη για να δεις πού πάει ο όγκος.',
            ],
            [
                'Read "Average Order Value" next to "Courier Cost / Revenue" — that pair tells you whether the channel makes money.',
                'Check "Orders Without a Carrier": a large number means you are understating shipping cost.',
                'Use the carrier and city breakdowns to see where the volume goes.',
            ],
        ),
        'columns': _tl(
            ['Κανάλι, μεταφορική, πόλη παράδοσης, μοντέλο εκτέλεσης.', 'Έσοδα, παραγγελίες, μέση παραγγελία και επιβαρύνσεις.'],
            ['Channel, carrier, delivery city, fulfilment model.', 'Revenue, orders, average order and surcharges.'],
        ),
        'popups': _tl(
            ['Popup παραστατικού.', 'Breakdown καναλιών, μεταφορικών και αποστολών.'],
            ['A document popup.', 'Breakdowns for channels, carriers and shipments.'],
        ),
        'checks': _tl(
            [
                'Η αναγνώριση e-shop βασίζεται στο mapping σειράς/καναλιού. Αν οι σειρές δεν είναι σωστά ρυθμισμένες, ο διαχωρισμός βγαίνει λάθος.',
                'Τα έξοδα αποστολής δεν είναι πωλούμενο είδος.',
                'Τιμολόγια courier που έρχονται συγκεντρωτικά δεν φαίνονται εδώ.',
            ],
            [
                'E-shop detection relies on series/channel mapping. If series are misconfigured, the split comes out wrong.',
                'Shipping charges are not a sold item.',
                'Consolidated courier invoices do not show up here.',
            ],
        ),
        'related': ['sales-analytics', 'sales-documents'],
    },
    {
        'id': 'sellout',
        'also_kpis': ['sell_through', 'days_of_supply', 'gmroi', 'qty_on_hand', 'margin_pct', 'gross_profit'],
        'title': _t('Sell Out', 'Sell Out'),
        'route': '/tenant/exports/sellout',
        'group': _t('Αναλύσεις → Sell Out', 'Analytics → Sell Out'),
        'shot': 'sellout',
        'purpose': _t(
            'Μετατρέπει πωλήσεις, απόθεμα και περιθώριο σε εμπορικές ενέργειες: παραγγελία, κίνδυνος έλλειψης, '
            'υπεραπόθεμα, χαμηλό περιθώριο, GMROI και ανακατανομές.',
            'Turns sales, stock and margin into commercial actions: reorder, stockout risk, overstock, '
            'low margin, GMROI and redistribution.',
        ),
        'steps': _tl(
            [
                'Ξεκίνα από τα stockout και reorder — είναι οι ενέργειες με άμεση απώλεια τζίρου.',
                'Μετά δες το χαμηλό περιθώριο: αυτά τα είδη θέλουν εμπορικό έλεγχο τιμής ή έκπτωσης.',
                'Τέλος, υπεραπόθεμα και ανακατανομές: εκεί είναι δεσμευμένο κεφάλαιο.',
                'Το GMROI είναι ο πιο αξιόπιστος δείκτης για απόφαση destocking.',
            ],
            [
                'Start with stockouts and reorders — those are the actions with immediate lost revenue.',
                'Then look at low margin: those items need a commercial review of price or discount.',
                'Finally overstock and redistribution: that is where capital is tied up.',
                'GMROI is the most reliable metric for a destocking decision.',
            ],
        ),
        'columns': _tl(
            [
                'Πίνακας ανά είδος με πωλήσεις, απόθεμα, περιθώριο, GMROI και πρόταση.',
                'Κέντρο αποφάσεων με λίστες ενεργειών.',
                'Γράφημα τάσης και εξαγωγή.',
            ],
            [
                'A per-item table with sales, stock, margin, GMROI and a suggestion.',
                'A decision centre with action lists.',
                'A trend chart and export.',
            ],
        ),
        'popups': _tl(
            [
                'Τα κουμπιά μεγέθυνσης ανοίγουν πλήρη λίστα ενεργειών.',
                'Διπλό κλικ σε γραμμή ανοίγει καρτέλα είδους.',
                'Εξαγωγή Excel/CSV ανά ενέργεια.',
            ],
            [
                'The expand buttons open the full action list.',
                'Double-click a row to open the item card.',
                'Excel/CSV export per action.',
            ],
        ),
        'checks': _tl(
            [
                'Το sell through 100% δεν σημαίνει πάντα επιτυχία — μπορεί απλώς να έχεις μείνει χωρίς απόθεμα.',
                'Η αναφορά αφορά συγκεκριμένο scope ειδών, όχι τον συνολικό τζίρο.',
            ],
            [
                '100% sell-through is not always success — you may simply have run out of stock.',
                'The report covers a specific item scope, not total turnover.',
            ],
        ),
        'related': ['inventory-analytics', 'destocking', 'price-control'],
    },
    {
        'id': 'purchases-analytics',
        'title': _t('Ανάλυση Αγορών', 'Purchases Analytics'),
        'route': '/tenant/purchases',
        'group': _t('Αναλύσεις → Αγορές', 'Analytics → Purchases'),
        'shot': 'purchases-analytics',
        'purpose': _t(
            'Αναλύει δαπάνη, προμηθευτές, εκπτώσεις, κόστος και συγκέντρωση αγορών.',
            'Analyses spend, suppliers, discounts, cost and purchasing concentration.',
        ),
        'steps': _tl(
            [
                'Δες το ζεύγος «Σύνολο Αγορών» και «Κόστος Αγορών»: η διαφορά τους είναι ακριβώς η έκπτωση που πέτυχες.',
                'Έλεγξε τη συγκέντρωση δαπάνης στους κορυφαίους προμηθευτές.',
                'Σύγκρινε την «Αγορασμένη Ποσότητα» με την πωληθείσα για να δεις αν χτίζεις απόθεμα.',
            ],
            [
                'Read the pair "Total Purchases" and "Purchase Cost": the gap between them is exactly the discount you achieved.',
                'Check how concentrated your spend is on the top suppliers.',
                'Compare "Units Purchased" against units sold to see whether stock is building.',
            ],
        ),
        'columns': _tl(
            [
                'Προμηθευτής, υποκατάστημα, αποθήκη, σειρά.',
                'Καθαρή αξία, έξοδα, ΦΠΑ και σύνολο.',
                'Κορυφαίοι προμηθευτές και συγκέντρωση δαπάνης.',
            ],
            [
                'Supplier, branch, warehouse, series.',
                'Net value, charges, VAT and total.',
                'Top suppliers and spend concentration.',
            ],
        ),
        'popups': _tl(
            ['KPI popups, πίνακες προμηθευτών και εξαγωγές για έλεγχο συμφωνίας.'],
            ['KPI popups, supplier tables and exports for reconciliation.'],
        ),
        'checks': _tl(
            [
                'Το ποσοστό έκπτωσης πιάνει μόνο εκπτώσεις γραμμής. Πιστώσεις τζίρου και rebates φαίνονται στις Συμφωνίες Προμηθευτών.',
                'Τα πιστωτικά αφαιρούν από την καθαρή εικόνα.',
            ],
            [
                'The discount percentage only captures line discounts. Volume rebates appear under Supplier Agreements.',
                'Credit notes subtract from the net picture.',
            ],
        ),
        'related': ['purchase-documents', 'supplier-targets', 'price-control'],
    },
    {
        'id': 'inventory-analytics',
        'title': _t('Ανάλυση Αποθέματος', 'Inventory Analytics'),
        'route': '/tenant/inventory',
        'group': _t('Αναλύσεις → Απόθεμα', 'Analytics → Inventory'),
        'shot': 'inventory-analytics',
        'purpose': _t(
            'Αξία και ποσότητα αποθέματος, δεσμεύσεις, κόστος, ταχυκίνηση και εμπορικός κίνδυνος stock.',
            'Stock value and quantity, reservations, cost, movement speed and commercial stock risk.',
        ),
        'steps': _tl(
            [
                'Διάλεξε ημερομηνία snapshot — το απόθεμα είναι φωτογραφία μιας στιγμής, όχι άθροισμα περιόδου.',
                'Δες την «Αξία Κτήσης Αποθήκης» και άνοιξέ την για ανάλυση χονδρικής, λιανικής και προοπτικού κέρδους.',
                'Σύγκρινε «Ποσότητα σε Απόθεμα» με «Δεσμευμένη Ποσότητα» για το πραγματικά διαθέσιμο.',
            ],
            [
                'Pick a snapshot date — stock is a point in time, not a period sum.',
                'Open "Warehouse Acquisition Value" for the wholesale / retail / prospective-profit breakdown.',
                'Compare "Quantity On Hand" against "Reserved Quantity" for what is genuinely available.',
            ],
        ),
        'columns': _tl(
            [
                'Αποθήκη, κατηγορία, brand, προμηθευτής.',
                'Ποσότητες, αξίες, κόστος και status.',
                'Πίνακες ταχυκίνητων και αργοκίνητων ειδών.',
            ],
            [
                'Warehouse, category, brand, supplier.',
                'Quantities, values, cost and status.',
                'Fast- and slow-moving item tables.',
            ],
        ),
        'popups': _tl(
            ['Popup αξίας αποθέματος με ανάλυση.', 'Detail ανά είδος / αποθήκη όπου υπάρχει.'],
            ['A stock-value popup with its breakdown.', 'Per-item / per-warehouse detail where available.'],
        ),
        'checks': _tl(
            [
                'Δύο snapshots δεν αθροίζονται.',
                'Έλεγξε είδη με απόθεμα και χωρίς πωλήσεις — εκεί είναι το δεσμευμένο κεφάλαιο.',
            ],
            [
                'Two snapshots do not add up.',
                'Look for items with stock but no sales — that is where capital sits idle.',
            ],
        ),
        'related': ['warehouse-items', 'sellout', 'destocking', 'replenishment'],
    },
    {
        'id': 'warehouse-items',
        'title': _t('Είδη Αποθήκης', 'Item Master'),
        'route': '/tenant/items',
        'group': _t('Αναλύσεις → Είδη', 'Analytics → Items'),
        'shot': 'warehouse-items',
        'purpose': _t(
            'Το αρχείο ειδών: κωδικός, barcode, κατηγορία, brand, προμηθευτής, ενεργότητα, κινητικότητα και ABC.',
            'The item file: code, barcode, category, brand, supplier, active flag, movement and ABC.',
        ),
        'steps': _tl(
            [
                'Χρησιμοποίησε το barcode για ακριβή αναζήτηση όταν ξέρεις το φυσικό προϊόν.',
                'Φίλτραρε ανά κατηγορία, brand ή προμηθευτή για να δουλέψεις σε ένα υποσύνολο.',
                'Δες το «Πουλήθηκαν σε περίοδο» αντί για το «Ενεργά Είδη» όταν σε ενδιαφέρει η πραγματική κίνηση.',
            ],
            [
                'Search by barcode for an exact match when you have the physical product.',
                'Filter by category, brand or supplier to work on a subset.',
                'Use "Sold in Period" rather than "Active Items" when you care about real movement.',
            ],
        ),
        'columns': _tl(
            ['Κωδικός, barcode, περιγραφή, κατηγορία, brand, προμηθευτής.', 'Status, κινητικότητα, ABC και εμπορικό status.'],
            ['Code, barcode, description, category, brand, supplier.', 'Status, movement, ABC and commercial status.'],
        ),
        'popups': _tl(
            ['Detail είδους, KPI popups και εξαγωγές λίστας.'],
            ['Item detail, KPI popups and list exports.'],
        ),
        'checks': _tl(
            [
                '«Ενεργό» είναι διαχειριστικό flag του SoftOne, όχι απόδειξη κίνησης.',
                'Το ABC και το εμπορικό status ορίζονται αποκλειστικά στο SoftOne — αν λείπουν, διορθώνονται εκεί.',
            ],
            [
                '"Active" is a SoftOne administrative flag, not proof the item moves.',
                'ABC and commercial status are set exclusively in SoftOne — if missing, fix them there.',
            ],
        ),
        'related': ['inventory-analytics', 'price-control', 'sellout'],
    },
    {
        'id': 'replenishment',
        'also_kpis': ['weeks_of_stock', 'fnr_items_in_need', 'qty_on_hand'],
        'title': _t('Replenishment / Availability', 'Replenishment / Availability'),
        'route': '/tenant/replenishment',
        'group': _t('Αναλύσεις → Replenishment', 'Analytics → Replenishment'),
        'shot': 'replenishment',
        'purpose': _t(
            'Πρόταση αναπλήρωσης και έλεγχος διαθεσιμότητας με βάση πωλήσεις, απόθεμα, αναμενόμενα από '
            'προμηθευτές, target stock και overstock.',
            'A replenishment proposal and availability check built on sales, stock, expected supplier '
            'receipts, target stock and overstock.',
        ),
        'steps': _tl(
            [
                'Φίλτραρε πρώτα σε σημείο, κατηγορία και προμηθευτή — οι προτάσεις υπολογίζονται πάνω στο επιλεγμένο scope.',
                'Δες τις «Εβδομάδες Κάλυψης» για να καταλάβεις πόσο πιεστική είναι η ανάγκη.',
                'Έλεγξε τις ανοιχτές παραγγελίες προμηθευτών πριν αποδεχτείς πρόταση.',
                'Κοίτα το Data Quality: γραμμές χωρίς τιμή αγοράς ή MOQ δεν υπολογίζονται αξιόπιστα.',
            ],
            [
                'Filter to a site, category and supplier first — suggestions are computed over the selected scope.',
                'Read "Weeks of Stock" to judge how urgent the need is.',
                'Check open supplier orders before accepting a suggestion.',
                'Watch Data Quality: rows without a purchase price or MOQ cannot be computed reliably.',
            ],
        ),
        'columns': _tl(
            [
                'Προϊόν, κατηγορίες, status, πωλήσεις, απόθεμα, εβδομάδες κάλυψης και target.',
                'Ανάγκες και overstock ανά φυσικό σημείο.',
                'Προτεινόμενη ποσότητα, Vendor MOQ και αξία.',
            ],
            [
                'Product, categories, status, sales, stock, weeks of cover and target.',
                'Needs and overstock per physical site.',
                'Suggested quantity, vendor MOQ and value.',
            ],
        ),
        'popups': _tl(
            [
                'Drill-down διαθεσιμότητας ανά σημείο, status, κατηγορία και προμηθευτή.',
                'Κορυφαίες ανάγκες προμηθευτή και κορυφαίο overstock.',
                'Έλεγχος ποιότητας δεδομένων με πεδίο και μήνυμα.',
            ],
            [
                'An availability drill-down by site, status, category and supplier.',
                'Top supplier needs and top overstock.',
                'A data-quality check with the offending field and message.',
            ],
        ),
        'checks': _tl(
            [
                'Το BI δεν κρατά backorders: μετασχηματισμένη παραγγελία θεωρείται κλειστή.',
                'Είδη χωρίς πωλήσεις δίνουν τεράστια κάλυψη — δες τα στο Destocking, όχι εδώ.',
            ],
            [
                'BI does not track backorders: a transformed order counts as closed.',
                'Items with no sales show enormous coverage — review those in Destocking, not here.',
            ],
        ),
        'related': ['fnr', 'supplier-orders', 'inventory-analytics'],
    },
    {
        'id': 'fnr',
        'title': _t('FnR Worksheet', 'FnR Worksheet'),
        'route': '/tenant/fnr',
        'group': _t('Αναλύσεις → FnR', 'Analytics → FnR'),
        'shot': 'fnr',
        'purpose': _t(
            'Παράγει worksheet παραγγελίας προμηθευτή με τη δομή και τη λογική του Excel FnR. Τα μπλε πεδία '
            'λειτουργούν ως φίλτρα και παράμετροι, και τα αποτελέσματα υπολογίζονται από τα BI δεδομένα.',
            'Produces a supplier order worksheet with the structure and logic of the FnR spreadsheet. The blue '
            'fields act as filters and parameters, and the results are computed from BI data.',
        ),
        'steps': _tl(
            [
                'Διάλεξε σημείο, κατηγορίες και προμηθευτή στα μπλε πεδία.',
                'Ρύθμισε Target Stock, Overstock και μέσους όρους εβδομάδων — αυτές οι παράμετροι αλλάζουν άμεσα το αποτέλεσμα.',
                'Έλεγξε «Γραμμές Παραγγελίας» και «Αξία Παραγγελίας» πριν κάνεις εξαγωγή.',
                'Κάνε εξαγωγή σε Excel για αποστολή στον προμηθευτή.',
            ],
            [
                'Pick the site, categories and supplier in the blue fields.',
                'Set Target Stock, Overstock and the week averages — these parameters change the result directly.',
                'Check "Order Rows" and "Order Value" before exporting.',
                'Export to Excel to send to the supplier.',
            ],
        ),
        'columns': _tl(
            [
                'Κωδικός, περιγραφή, κατηγορίες, status και προμηθευτής.',
                'Πωλήσεις, απόθεμα, εβδομάδες κάλυψης, αναμενόμενα, target, ανάγκη και overstock ανά σημείο.',
                'Τελική ποσότητα και αξία παραγγελίας.',
            ],
            [
                'Code, description, categories, status and supplier.',
                'Sales, stock, weeks of cover, expected receipts, target, need and overstock per site.',
                'Final order quantity and value.',
            ],
        ),
        'popups': _tl(
            ['Φίλτρα πολλαπλής επιλογής για σημείο, κατηγορίες και προμηθευτή.', 'Εξαγωγή Excel με ανθρώπινη μορφοποίηση.'],
            ['Multi-select filters for site, categories and supplier.', 'An Excel export with human formatting.'],
        ),
        'checks': _tl(
            [
                'Τα φίλτρα εφαρμόζονται ΠΡΙΝ τον υπολογισμό των KPI.',
                'Το Vendor MOQ μπορεί να ανεβάσει την ποσότητα πάνω από την καθαρή ανάγκη.',
                'Είδη χωρίς τιμή αγοράς δεν αποτιμώνται, οπότε η αξία παραγγελίας υποεκτιμάται.',
            ],
            [
                'Filters are applied BEFORE the KPIs are computed.',
                'Vendor MOQ can push the quantity above the pure need.',
                'Items with no purchase price are not valued, so the order value is understated.',
            ],
        ),
        'related': ['replenishment', 'supplier-orders', 'availability'],
    },
    {
        'id': 'availability',
        'also_kpis': ['weeks_of_stock', 'items_abc', 'items_commercial_status'],
        'title': _t('Availability Brief', 'Availability Brief'),
        'route': '/tenant/availability',
        'group': _t('Αναλύσεις → Availability', 'Analytics → Availability'),
        'shot': 'availability',
        'purpose': _t(
            'Αναπαράγει το Availability brief με Table, Trends, Correlation και Recommendations, υπολογισμένα από τα κυκλώματα.',
            'Reproduces the Availability brief with Table, Trends, Correlation and Recommendations, computed from the streams.',
        ),
        'steps': _tl(
            [
                'Όρισε τα φίλτρα στα μπλε πεδία (σημείο, κατηγορίες, προμηθευτής, status).',
                'Δες το Table για την εικόνα ανά ABCD και εμπορικό status.',
                'Πήγαινε στα Trends και στο Correlation για τη σχέση διαθεσιμότητας και πωλήσεων.',
                'Κλείσε με τα Recommendations και εξαγωγή σε Excel.',
            ],
            [
                'Set the filters in the blue fields (site, categories, supplier, status).',
                'Use Table for the picture by ABCD and commercial status.',
                'Move to Trends and Correlation for the relationship between availability and sales.',
                'Finish with Recommendations and an Excel export.',
            ],
        ),
        'columns': _tl(
            [
                'Πίνακας ανά ABCD και εμπορικό status.',
                'Trends ανά σημείο και σύνολο.',
                'Correlation διαθεσιμότητας με πωλήσεις έναντι προηγούμενου έτους.',
            ],
            [
                'A table by ABCD and commercial status.',
                'Trends per site and in total.',
                'Correlation of availability with sales against last year.',
            ],
        ),
        'popups': _tl(
            ['Tabs Table, Trends, Correlation και Recommendations, με εξαγωγή Excel.'],
            ['Table, Trends, Correlation and Recommendations tabs, with Excel export.'],
        ),
        'checks': _tl(
            [
                'Η σύνοψη πάνω από τα γραφήματα είναι υπολογισμένο αποτέλεσμα, όχι στατική εικόνα.',
                'Τα φίλτρα έρχονται από την υποδομή BI, όχι από Excel.',
            ],
            [
                'The summary above the charts is a computed result, not a static image.',
                'The filters come from the BI infrastructure, not from a spreadsheet.',
            ],
        ),
        'related': ['replenishment', 'fnr', 'destocking'],
    },
    {
        'id': 'destocking',
        'also_kpis': ['gmroi', 'sell_through', 'days_of_supply', 'stock_value'],
        'title': _t('Destocking Brief', 'Destocking Brief'),
        'route': '/tenant/destocking',
        'group': _t('Αναλύσεις → Destocking', 'Analytics → Destocking'),
        'shot': 'destocking',
        'purpose': _t(
            'Υπολογίζει overstock, περιθώριο, D3 και προτάσεις αποθεματοποίησης από τα BI δεδομένα.',
            'Computes overstock, margin, D3 and destocking recommendations from BI data.',
        ),
        'steps': _tl(
            [
                'Όρισε τις δύο ημερομηνίες αποθέματος και το threshold.',
                'Δες το «Total Overstock» για τη συνολική αξία υπερ-αποθέματος.',
                'Πήγαινε στα Recommendations για τις προτάσεις ανά είδος και κάνε εξαγωγή.',
            ],
            [
                'Set the two stock dates and the threshold.',
                'Read "Total Overstock" for the total value of excess stock.',
                'Go to Recommendations for the per-item proposals and export.',
            ],
        ),
        'columns': _tl(
            [
                'Table ανά ABCD και εμπορικό status.',
                'Trends με A over, B over, C, D και D3 ανά περίοδο.',
                'Correlation overstock, D3 overstock και margin.',
            ],
            [
                'A table by ABCD and commercial status.',
                'Trends with A over, B over, C, D and D3 per period.',
                'Correlation of overstock, D3 overstock and margin.',
            ],
        ),
        'popups': _tl(
            ['Tabs Table, Trends, Correlation και Recommendations, με εξαγωγή Excel.'],
            ['Table, Trends, Correlation and Recommendations tabs, with Excel export.'],
        ),
        'checks': _tl(
            [
                'Το D3 εμφανίζεται ξεχωριστά από το D στα trends και στο correlation.',
                'Το threshold και οι ημερομηνίες αποθέματος είναι χειροκίνητες παράμετροι και αλλάζουν το αποτέλεσμα.',
            ],
            [
                'D3 is shown separately from D in both trends and correlation.',
                'The threshold and stock dates are manual parameters and they change the result.',
            ],
        ),
        'related': ['sellout', 'inventory-analytics', 'availability'],
    },
    {
        'id': 'price-control',
        'title': _t('Έλεγχος Τιμών', 'Price Control'),
        'route': '/tenant/price-control',
        'group': _t('Αναλύσεις → Έλεγχος Τιμών', 'Analytics → Price Control'),
        'shot': 'price-control',
        'purpose': _t(
            'Έλεγχος τιμών, περιθωρίων και αποκλίσεων με βάση την πραγματική καθαρή τιμή κτήσης, τη λιανική '
            'SoftOne με ΦΠΑ και το επιθυμητό μεικτό περιθώριο.',
            'Reviews prices, margins and deviations against the real net acquisition price, the SoftOne retail '
            'price including VAT, and your target gross margin.',
        ),
        'steps': _tl(
            [
                'Όρισε τον «Στόχο Μικτού Περιθωρίου» — από αυτόν υπολογίζεται η Τιμή Στόχου κάθε είδους.',
                'Φίλτραρε σε προμηθευτή, κατηγορία ή ομάδα για να δουλέψεις σε διαχειρίσιμο σύνολο.',
                'Πάτα «Κάτω από Προβλεπόμενη» για τη λίστα ειδών που δεν πιάνουν τον στόχο.',
                'Ξεκίνα τις διορθώσεις από τα είδη με μεγάλο τζίρο και χαμηλό περιθώριο.',
            ],
            [
                'Set the "Target Gross Margin" — every item\'s Target Price is derived from it.',
                'Filter to a supplier, category or group to work on a manageable set.',
                'Click "Below Target" for the list of items missing the target.',
                'Start fixing with the high-turnover, low-margin items.',
            ],
        ),
        'columns': _tl(
            [
                'Κωδικός, περιγραφή, barcode, προμηθευτής, brand, κατηγορία και ομάδα.',
                'Τιμή χονδρικής, τιμή κτήσης, τιμή λιανικής, τιμή στόχου και μέση τιμή πώλησης.',
                'Περιθώριο λιανικής και προτεινόμενη έκπτωση. Ο πίνακας έχει οριζόντια κύλιση και sticky header.',
            ],
            [
                'Code, description, barcode, supplier, brand, category and group.',
                'Wholesale price, acquisition price, retail price, target price and average selling price.',
                'Retail margin and suggested discount. The table scrolls horizontally with a sticky header.',
            ],
        ),
        'popups': _tl(
            [
                'Τα «Πάνω / Κάτω από Προβλεπόμενη» ανοίγουν αναλυτική λίστα ειδών με τιμή κτήσης, στόχου, λιανικής και διαφορά.',
                'Detail είδους και εξαγωγές για εμπορική επεξεργασία.',
            ],
            [
                '"Above / Below Target" open an item-level list with acquisition, target and retail price plus the gap.',
                'Item detail and exports for commercial work.',
            ],
        ),
        'checks': _tl(
            [
                'Η τιμή κτήσης ΔΕΝ είναι η τελευταία τιμή αγοράς — είναι αξία αγορών / συνολικές ποσότητες στην περίοδο.',
                'Ο στόχος είναι περιθώριο πάνω στην τιμή πώλησης, όχι markup πάνω στο κόστος.',
                'Αν λείπει η λιανική, έλεγξε τον τελευταίο συγχρονισμό αποθέματος.',
                'Μην αλλάζεις τιμή χωρίς έλεγχο κατηγορίας και ανταγωνισμού.',
            ],
            [
                'The acquisition price is NOT the last purchase price — it is purchase value / total quantity over the period.',
                'The target is a margin on the selling price, not a markup on cost.',
                'If the retail price is missing, check the last stock sync.',
                'Do not reprice without checking the category and the competition.',
            ],
        ),
        'related': ['purchases-analytics', 'sellout', 'warehouse-items'],
    },
    {
        'id': 'supplier-targets',
        'title': _t('Συμφωνίες Προμηθευτών', 'Supplier Agreements'),
        'route': '/tenant/supplier-targets',
        'group': _t('Αναλύσεις → Συμφωνίες Προμηθευτών', 'Analytics → Supplier Agreements'),
        'shot': 'supplier-targets',
        'purpose': _t(
            'Διαχείριση στόχων προμηθευτών, συμμετεχόντων ειδών, επιστροφών και προόδου συμφωνίας.',
            'Manage supplier targets, participating items, rebates and agreement progress.',
        ),
        'steps': _tl(
            [
                'Δες την πρόοδο κάθε ενεργής συμφωνίας απέναντι στον στόχο.',
                'Άνοιξε τη συμφωνία για να ελέγξεις ποια είδη συμμετέχουν.',
                'Για νέα περίοδο, κάνε αντιγραφή της συμφωνίας — μην επεξεργάζεσαι ληγμένη.',
            ],
            [
                'Check each active agreement\'s progress against its target.',
                'Open an agreement to review which items participate.',
                'For a new period, copy the agreement — do not edit an expired one.',
            ],
        ),
        'columns': _tl(
            [
                'Προμηθευτής, περίοδος, στόχος, τύπος επιστροφής και είδη.',
                'Κατάσταση ενεργή / ληγμένη και ενέργειες αντιγραφής, εκτύπωσης, email.',
            ],
            [
                'Supplier, period, target, rebate type and items.',
                'Active / expired status and copy, print and email actions.',
            ],
        ),
        'popups': _tl(
            ['Detail συμφωνίας, επεξεργασία ενεργής συμφωνίας, εκτύπωση ή αποστολή email.'],
            ['Agreement detail, editing an active agreement, printing or emailing it.'],
        ),
        'checks': _tl(
            [
                'Επιβεβαίωσε τις ημερομηνίες ισχύος πριν μετρήσεις πρόοδο.',
                'Έλεγξε ότι τα συμμετέχοντα είδη είναι σωστά — αλλιώς η πρόοδος βγαίνει λάθος.',
            ],
            [
                'Confirm the validity dates before reading progress.',
                'Verify the participating items — otherwise the progress is wrong.',
            ],
        ),
        'related': ['purchases-analytics', 'purchase-documents'],
    },
    {
        'id': 'era-exploration-data',
        'title': _t('eRA Exploration Data', 'eRA Exploration Data'),
        'route': '/tenant/era-exploration-data',
        'group': _t('Αναλύσεις → eRA', 'Analytics → eRA'),
        'shot': 'era-exploration-data',
        'purpose': _t(
            'Ανάλυση εξωτερικού αρχείου αγοράς eRA: μερίδιο αγοράς, δικές μας πωλήσεις, barcodes, μέση τιμή '
            'και ευκαιρίες προσθήκης ειδών.',
            'Analysis of the external eRA market file: market share, our own sales, barcodes, average price '
            'and assortment opportunities.',
        ),
        'steps': _tl(
            [
                'Ανέβασε το αρχείο eRA από τις Ρυθμίσεις tenant.',
                'Δες το «Μερίδιο Αξίας» για τη θέση σου στο δείγμα αγοράς.',
                'Άνοιξε τις «Προτάσεις Προσθήκης» για είδη με σημαντικό μερίδιο που δεν έχεις.',
                'Διπλό κλικ σε γραμμή δείχνει όλα τα barcodes του προϊόντος.',
            ],
            [
                'Upload the eRA file from tenant Settings.',
                'Read "Value Market Share" for your position in the market sample.',
                'Open "Assortment Suggestions" for meaningful-share items you do not carry.',
                'Double-click a row to see all of a product\'s barcodes.',
            ],
        ),
        'columns': _tl(
            [
                'Προϊόν, κύριο barcode και όλα τα barcodes στο detail.',
                'Brand, κατηγορία eRA, αξίες, μονάδες, μερίδιο και μέση τιμή.',
            ],
            [
                'Product, primary barcode and all barcodes in the detail view.',
                'Brand, eRA category, values, units, share and average price.',
            ],
        ),
        'popups': _tl(
            ['Με κλικ στα KPI ανοίγουν τα αντίστοιχα είδη· popup ελέγχου τιμής και πρότασης προσθήκης.'],
            ['Clicking a KPI opens the matching items; popups for price checks and assortment suggestions.'],
        ),
        'checks': _tl(
            [
                'Πρόταση προσθήκης δεν είναι κάθε είδος που λείπει, αλλά είδος με σημαντικό μερίδιο αγοράς.',
                'Έλεγξε την εποχικότητα από τον μήνα του αρχείου.',
            ],
            [
                'A suggestion is not every missing item, only one with meaningful market share.',
                'Check seasonality via the file\'s month.',
            ],
        ),
        'related': ['iqvia', 'warehouse-items'],
    },
    {
        'id': 'iqvia',
        'title': _t('IQVIA Market Data', 'IQVIA Market Data'),
        'route': '/tenant/iqvia',
        'group': _t('Αναλύσεις → IQVIA', 'Analytics → IQVIA'),
        'shot': 'iqvia',
        'purpose': _t(
            'Μηνιαία ανάλυση αγοράς IQVIA με ιστορικό, τάσεις, εποχικότητα και συγκρίσεις.',
            'Monthly IQVIA market analysis with history, trends, seasonality and comparisons.',
        ),
        'steps': _tl(
            [
                'Ανέβασε το μηνιαίο αρχείο IQVIA — κάθε αρχείο αφορά συγκεκριμένο μήνα.',
                'Δες αξία, μονάδες και μέση τιμή για το μέγεθος της αγοράς.',
                'Χρησιμοποίησε τα breakdowns για κορυφαίες κατηγορίες, κατασκευαστές και περιοχές.',
            ],
            [
                'Upload the monthly IQVIA file — each file covers one specific month.',
                'Read value, units and average price for the size of the market.',
                'Use the breakdowns for top categories, manufacturers and territories.',
            ],
        ),
        'columns': _tl(
            [
                'Προϊόν και pack.',
                'CATEGORY, ATC3, OTC3, corporation, manufacturer και territory.',
                'Μονάδες, αξία, μέση τιμή και μερίδιο.',
            ],
            [
                'Product and pack.',
                'CATEGORY, ATC3, OTC3, corporation, manufacturer and territory.',
                'Units, value, average price and share.',
            ],
        ),
        'popups': _tl(
            ['Breakdowns ανά κατηγορία, κατασκευαστή και περιοχή.'],
            ['Breakdowns by category, manufacturer and territory.'],
        ),
        'checks': _tl(
            [
                'Το report διαβάζει από τα εισαγόμενα BI tables, όχι από το Excel.',
                'Για σωστές τάσεις πρέπει να ανεβαίνουν όλα τα μηνιαία αρχεία με συνέπεια.',
            ],
            [
                'The report reads from the imported BI tables, not from the spreadsheet.',
                'Trends only work if every monthly file is uploaded consistently.',
            ],
        ),
        'related': ['era-exploration-data'],
    },

    # ---------------- Συγκρίσεις / εξαγωγές / ρυθμίσεις -------------------
    {
        'id': 'comparisons',
        'also_kpis': ['growth_vs_prev', 'sales_period'],
        'title': _t('Συγκρίσεις', 'Comparisons'),
        'route': '/tenant/comparisons/period-vs-period',
        'group': _t('Συγκρίσεις', 'Comparisons'),
        'shot': 'comparisons',
        'purpose': _t(
            'Σύγκριση περιόδου με περίοδο, υποκαταστήματος με υποκατάστημα και κατηγορίας με κατηγορία.',
            'Period against period, branch against branch, category against category.',
        ),
        'steps': _tl(
            [
                'Διάλεξε τον τύπο σύγκρισης από το μενού.',
                'Όρισε τα δύο σκέλη (Α και Β) — φρόντισε να έχουν το ίδιο μήκος περιόδου.',
                'Διάβασε τη μεταβολή σε αξία ΚΑΙ σε ποσοστό: το ποσοστό παραπλανά όταν η βάση είναι μικρή.',
            ],
            [
                'Pick the comparison type from the menu.',
                'Set both sides (A and B) — make sure the periods are the same length.',
                'Read the change in value AND in percent: the percentage misleads when the base is small.',
            ],
        ),
        'columns': _tl(
            ['Μεταβολή αξίας και ποσοστού ανά διάσταση.', 'Συμμετοχή κάθε διάστασης στο σύνολο.'],
            ['Value and percentage change per dimension.', 'Each dimension\'s share of the total.'],
        ),
        'popups': _tl(
            ['Drill-down ανά διάσταση όπου υπάρχει.'],
            ['Drill-down per dimension where available.'],
        ),
        'checks': _tl(
            ['Σύγκρινε πάντα ίδιου μήκους περιόδους.', 'Αν το σκέλος σύγκρισης είναι μηδενικό, το ποσοστό δεν ορίζεται.'],
            ['Always compare periods of equal length.', 'If the comparison side is zero, the percentage is undefined.'],
        ),
        'related': ['sales-analytics', 'dashboard'],
    },
    {
        'id': 'store-dashboard',
        'title': _t('Κατάστημα', 'Store'),
        'route': '/tenant/store-dashboard',
        'group': _t('Πίνακες', 'Dashboards'),
        'shot': 'store-dashboard',
        'purpose': _t(
            'Prescriptive πίνακας ανά κατάστημα: τι συμβαίνει και τι να κάνεις, κατά € αξία.',
            'A prescriptive per-store cockpit: what is happening and what to do, ranked by € value.',
        ),
        'steps': _tl(
            [
                'Διάλεξε κατάστημα και περίοδο.',
                'Δες τα KPIs + Store Health, μετά τις κάρτες δράσης: Χαμένες πωλήσεις (ταχυκίνητα εκτός αποθέματος → παραγγελία), Βαλτωμένο απόθεμα (δεσμευμένο κεφάλαιο → μεταφορά/έκπτωση), Κατηγορίες σε πτώση vs πέρσι.',
            ],
            [
                'Pick a store and period.',
                'Read the KPIs + Store Health, then the action cards: Lost sales (best-sellers out of stock → order), Dead stock (tied capital → move/discount), Declining categories vs last year.',
            ],
        ),
        'columns': _tl(['Λίστες ειδών ανά κάρτα, ταξινομημένες κατά € αξία.'], ['Item lists per card, ranked by € value.']),
        'popups': _tl(['—'], ['—']),
        'checks': _tl(['Το «Βαλτωμένο» βασίζεται στο τελευταίο snapshot αποθέματος.'], ['Dead stock uses the latest inventory snapshot.']),
        'related': ['replenishment', 'destocking'],
    },
    {
        'id': 'exports',
        'title': _t('Report Builder', 'Report Builder'),
        'route': '/tenant/exports/reports',
        'group': _t('Εξαγωγές', 'Exports'),
        'shot': 'exports',
        'purpose': _t(
            'Έτοιμες αναφορές και εξαγωγές σε CSV/Excel, με βάση τα ενεργά φίλτρα.',
            'Ready-made reports and CSV/Excel exports, driven by the active filters.',
        ),
        'steps': _tl(
            [
                'Όρισε ΠΡΩΤΑ την περίοδο (Από / Έως) και τα φίλτρα — η εξαγωγή ακολουθεί ό,τι βλέπεις στην οθόνη.',
                'Διαθέσιμα φίλτρα: Περίοδος (Από/Έως), Υποκατάστημα, Αποθηκευτικός χώρος, Brands, Ομάδα Ειδών και οι τρεις βασικές κατηγορίες του είδους (Κατηγορία 1, 2, 3). Κάθε φίλτρο δέχεται πολλές επιλογές.',
                'Οι Αναφορές και το CSV/Excel μοιράζονται τα ΙΔΙΑ φίλτρα — ό,τι επιλέγεις εφαρμόζεται με τον ίδιο τρόπο και στα δύο.',
                'Η λίστα ξεκινά κενή: συμπλήρωσε τα φίλτρα και πάτησε «Υπολογισμός» για να φορτώσουν τα αποτελέσματα· «Καθαρισμός» για μηδενισμό.',
                'Για αρχείο, πάτησε «Λήψη Excel» ή «Λήψη CSV» — κατεβάζει ολόκληρη τη λίστα με τα ίδια φίλτρα (η οθόνη δείχνει τα πρώτα 1000).',
                'Στο κάτω μέρος υπάρχει γραμμή ΣΥΝΟΛΟ (ποσότητα + αξία) για ΟΛΟ το φιλτραρισμένο σύνολο, όχι μόνο τα εμφανιζόμενα.',
                'Η «Ανά Κανάλι» δείχνει καθαρή αξία, τεμάχια, συνεισφορά % και margin % ανά κανάλι πώλησης (Site, Wolt, Skroutz κ.λπ.· οι πωλήσεις χωρίς online κανάλι = «Φυσικό κατάστημα»), με τα ίδια φίλτρα και download.',
                'Η «Σύγκριση Ομάδων» δείχνει τζίρο, κόστος και κέρδος ανά ομάδα ειδών για δύο περιόδους: Α = τρέχων μήνας, Β = περσινός αντίστοιχος μήνας (προσυμπληρωμένα, αλλάζονται ελεύθερα). Κόστος = Τζίρος − Κέρδος.',
                'Η «Ευέλικτη Ανάλυση» είναι ένα report που το φτιάχνεις μόνος σου: διάλεξε «Ομαδοποίηση κατά» (κανάλι/ομάδα/brand/κατηγορία/κατάστημα/αποθήκη), τύπο (ανάλυση περιόδου ή σύγκριση Α/Β) και πρόσθεσε στήλες με το κουμπί «Προσθήκη στήλης» επιλέγοντας από τη λίστα διαθέσιμων πεδίων (καθαρή αξία, τεμάχια, κόστος, κέρδος, margin %, παραστατικά, ΦΠΑ κ.ά.) — κάθε στήλη αφαιρείται με το ×· σύρε τα πλακίδια για να αλλάξεις τη σειρά των στηλών (ακολουθεί ο πίνακας και το export). Ίδια φίλτρα και download.',
            ],
            [
                'Set the period (From / To) and filters FIRST — the export follows what is on screen.',
                'Available filters: Period (From/To), Branch, Warehouse, Brands, Item Group and the three basic item categories (Category 1, 2, 3). Each filter accepts multiple selections.',
                'Reports and CSV/Excel share the SAME filters — a selection applies identically to both.',
                'The list starts empty: set the filters and press "Calculate" to load results; "Clear" to reset.',
                'For a file, press "Download Excel" or "Download CSV" — it downloads the full list with the same filters (the screen shows the first 1000).',
            ],
        ),
        'columns': _tl(
            [
                'Αποτέλεσμα ανά είδος: Όνομα, Barcode, Brand, Κατηγορία 1, 2, 3, Ποσότητα πωλήσεων (τεμ.) και Αξία.',
                'Ποσότητα/Αξία υπολογίζονται για το επιλεγμένο υποκατάστημα/αποθηκευτικό χώρο/περίοδο, με τις επιστροφές να αφαιρούνται (ίδιος κανόνας με τα dashboards).',
                'Οι κατηγορίες είναι αλληλεξαρτώμενες: επιλέγοντας Κατηγορία 1 περιορίζονται οι επιλογές της Κατηγορίας 2, και η Κατηγορία 2 περιορίζει την Κατηγορία 3.',
            ],
            [
                'Result per item: Name, Barcode, Brand, Category 1, 2, 3, sold Quantity (units) and Value.',
                'Quantity/Value are computed for the selected branch/warehouse/period, with returns subtracted (same rule as the dashboards).',
                'The categories cascade: picking Category 1 narrows Category 2, and Category 2 narrows Category 3.',
            ],
        ),
        'popups': _tl(
            ['Επιλογές μορφής και εύρους πριν την εξαγωγή.'],
            ['Format and range options before exporting.'],
        ),
        'checks': _tl(
            ['Η εξαγωγή δεν είναι πάντα ολόκληρο το dataset — ακολουθεί τα ενεργά φίλτρα.'],
            ['An export is not always the whole dataset — it follows the active filters.'],
        ),
        'related': ['sellout', 'fnr'],
    },
    {
        'id': 'tenant-users',
        'title': _t('Χρήστες & Άδειες', 'Users & Licences'),
        'route': '/tenant/users',
        'group': _t('Προφίλ → Χρήστες', 'Profile → Users'),
        'shot': 'tenant-users',
        'purpose': _t(
            'Διαχείριση χρηστών και διαθέσιμων αδειών ταυτόχρονης χρήσης από τον ίδιο τον πελάτη.',
            'Self-service management of users and available concurrent-use licences.',
        ),
        'steps': _tl(
            [
                'Δες πόσες άδειες έχεις και πόσες είναι δεσμευμένες.',
                'Δημιούργησε χρήστη — θα λάβει email πρόσκλησης με σύνδεσμο ορισμού κωδικού.',
                'Αν δεν έλαβε το email, χρησιμοποίησε την επαναποστολή πρόσκλησης.',
            ],
            [
                'Check how many licences you have and how many are taken.',
                'Create a user — they receive an invitation email with a set-password link.',
                'If the email did not arrive, use the resend-invitation action.',
            ],
        ),
        'columns': _tl(
            ['Ονοματεπώνυμο, email, ρόλος, επαγγελματικό προφίλ και κατάσταση.'],
            ['Name, email, role, professional profile and status.'],
        ),
        'popups': _tl(
            ['Μηνύματα επιτυχίας ή σφάλματος πάνω στη σελίδα.'],
            ['Success or error messages shown on the page.'],
        ),
        'checks': _tl(
            [
                'Δεν μπορείς να ενεργοποιήσεις χρήστη αν έχεις φτάσει το όριο αδειών.',
                'Μην απενεργοποιείς τον εαυτό σου αν είσαι ο μόνος διαχειριστής.',
                'Η πρόσκληση έχει χρονική λήξη.',
            ],
            [
                'You cannot activate a user once the licence limit is reached.',
                'Do not deactivate yourself if you are the only administrator.',
                'Invitations expire.',
            ],
        ),
        'related': ['tenant-profile', 'tenant-settings'],
    },
    {
        'id': 'tenant-settings',
        'title': _t('Ρυθμίσεις', 'Settings'),
        'route': '/tenant/settings',
        'group': _t('Προφίλ → Ρυθμίσεις', 'Profile → Settings'),
        'shot': 'tenant-settings',
        'purpose': _t(
            'Παράμετροι που επηρεάζουν λειτουργίες, όπως το ανέβασμα αρχείων eRA και το lookback παραγγελιών προμηθευτών.',
            'Parameters that drive behaviour, such as eRA file uploads and the supplier-order lookback.',
        ),
        'steps': _tl(
            [
                'Ανέβασε αρχεία αγοράς (eRA / IQVIA) από την αντίστοιχη ενότητα.',
                'Ρύθμισε το lookback ημερών για τις παραγγελίες προμηθευτών.',
                'Αποθήκευσε και επιβεβαίωσε το μήνυμα επιτυχίας.',
            ],
            [
                'Upload market files (eRA / IQVIA) from the matching section.',
                'Set the lookback days for supplier orders.',
                'Save and confirm the success message.',
            ],
        ),
        'columns': _tl(
            ['Φόρμες ρυθμίσεων ανά ενότητα.'],
            ['Settings forms grouped by section.'],
        ),
        'popups': _tl(
            ['Μηνύματα επιτυχίας ή αποτυχίας μετά την αποθήκευση.'],
            ['Success or failure messages after saving.'],
        ),
        'checks': _tl(
            [
                'Αν το αρχείο απορριφθεί, έλεγξε μορφή Excel και όνομα αρχείου.',
                'Η αλλαγή lookback επηρεάζει την προεπιλεγμένη περίοδο στις Παραγγελίες Προμηθευτών.',
            ],
            [
                'If a file is rejected, check the Excel format and the file name.',
                'Changing the lookback affects the default period on Supplier Orders.',
            ],
        ),
        'related': ['supplier-orders', 'era-exploration-data', 'iqvia'],
    },
    {
        'id': 'tenant-profile',
        'title': _t('Προφίλ Χρήστη', 'User Profile'),
        'route': '/tenant/profile',
        'group': _t('Προφίλ → Το προφίλ μου', 'Profile → My profile'),
        'shot': 'tenant-profile',
        'purpose': _t(
            'Τα βασικά σου στοιχεία: ονοματεπώνυμο, τηλέφωνο και ρόλος.',
            'Your basic details: name, phone and role.',
        ),
        'steps': _tl(
            ['Ενημέρωσε ονοματεπώνυμο και τηλέφωνο.', 'Αποθήκευσε — το αποτέλεσμα εμφανίζεται στη σελίδα.'],
            ['Update your name and phone number.', 'Save — the result is shown on the page.'],
        ),
        'columns': _tl(
            ['Ονοματεπώνυμο, email, τηλέφωνο και ρόλος.'],
            ['Name, email, phone and role.'],
        ),
        'popups': _tl(['Δεν χρησιμοποιεί KPI popup.'], ['No KPI popup on this page.']),
        'checks': _tl(
            ['Το email ταυτοποίησης δεν αλλάζει από εδώ.', 'Για αλλαγή ρόλου ή πρόσβασης χρειάζεται διαχειριστής tenant.'],
            ['Your sign-in email cannot be changed here.', 'Changing a role or access needs a tenant administrator.'],
        ),
        'related': ['tenant-users'],
    },
    {
        'id': 'tenant-messages',
        'title': _t('Μηνύματα / Ιστορικό', 'Messages / Activity'),
        'route': '/tenant/messages',
        'group': _t('Προφίλ → Μηνύματα', 'Profile → Messages'),
        'shot': 'tenant-messages',
        'purpose': _t(
            'Πρόσφατες ενέργειες και συμβάντα που αφορούν τον λογαριασμό σου ή την εταιρεία.',
            'Recent actions and events affecting your account or the company.',
        ),
        'steps': _tl(
            ['Χρησιμοποίησέ το για να δεις αν έγινε πρόσφατη αλλαγή χρήστη ή ρύθμισης.'],
            ['Use it to check whether a user or setting was recently changed.'],
        ),
        'columns': _tl(
            ['Ημερομηνία, ενέργεια, περιγραφή και σχετικό αντικείμενο.'],
            ['Date, action, description and the related object.'],
        ),
        'popups': _tl(['Δεν χρησιμοποιεί KPI popup.'], ['No KPI popup on this page.']),
        'checks': _tl(
            ['Για πλήρες audit απαιτείται πρόσβαση διαχειριστή.'],
            ['A full audit trail requires administrator access.'],
        ),
        'related': ['tenant-users'],
    },
)

CIRCUITS_BY_ID: dict[str, dict[str, Any]] = {c['id']: c for c in CIRCUITS}


# --------------------------------------------------------------------------
# "Πώς θα βρω…" — the task index
# --------------------------------------------------------------------------
#  Each task answers a question a user actually asks out loud. `route` is the
#  shortest path to the answer, `steps` the clicks, `why` the reason this is the
#  right screen and not a plausible-looking wrong one.

TASK_GROUPS: tuple[dict[str, Any], ...] = (
    {
        'id': 'daily',
        'title': _t('Καθημερινή δουλειά', 'Everyday work'),
        'icon': 'fe-sunrise',
        'tasks': (
            {
                'q': _t('Πόσο πούλησα σήμερα;', 'How much did I sell today?'),
                'route': '/tenant/dashboard',
                'circuit': 'dashboard',
                'steps': _tl(
                    ['Άνοιξε τον Πίνακα Διοίκησης.', 'Βάλε την ημερομηνία «Έως» στη σημερινή ημέρα.', 'Διάβασε την κάρτα «Τζίρος Ημέρας».'],
                    ['Open the Executive Dashboard.', 'Set the "To" date to today.', 'Read the "Day Turnover" card.'],
                ),
                'why': _t(
                    'Η κάρτα Ημέρας ακολουθεί την ημερομηνία «Έως», οπότε λειτουργεί και για οποιαδήποτε άλλη ημέρα θέλεις να ελέγξεις.',
                    'The Day card follows the "To" date, so the same trick works for any other day you want to check.',
                ),
            },
            {
                'q': _t('Πώς πάω σε σχέση με πέρσι;', 'How am I doing against last year?'),
                'route': '/tenant/dashboard',
                'circuit': 'dashboard',
                'steps': _tl(
                    ['Στον Πίνακα Διοίκησης δες τις κάρτες «Τζίρος (YTD)» τρέχοντος και προηγούμενου έτους.', 'Σύγκρινε YTD με YTD, όχι YTD με FULL YEAR.'],
                    ['On the Executive Dashboard look at the current and previous year "Turnover (YTD)" cards.', 'Compare YTD against YTD, not YTD against FULL YEAR.'],
                ),
                'why': _t(
                    'Υπάρχουν δύο κάρτες προηγούμενου έτους: η YTD κόβει στην ίδια ημέρα, η FULL YEAR είναι ολόκληρο το έτος.',
                    'There are two previous-year cards: YTD cuts at the same day, FULL YEAR is the whole year.',
                ),
            },
            {
                'q': _t('Τι πρέπει να προσέξω σήμερα;', 'What needs my attention today?'),
                'route': '/tenant/insights',
                'circuit': 'insights',
                'steps': _tl(
                    ['Άνοιξε τα Insights.', 'Ξεκίνα από τη σοβαρότητα «high».', 'Πάτα drill-down σε κάθε ένα πριν αποφασίσεις.'],
                    ['Open Insights.', 'Start with "high" severity.', 'Drill into each one before deciding.'],
                ),
                'why': _t(
                    'Τα Insights είναι το μόνο σημείο που σαρώνει όλα τα κυκλώματα και σου φέρνει μόνο τις αποκλίσεις.',
                    'Insights is the only place that scans every stream and surfaces only the exceptions.',
                ),
            },
        ),
    },
    {
        'id': 'sales',
        'title': _t('Πωλήσεις και τζίρος', 'Sales and turnover'),
        'icon': 'fe-trending-up',
        'tasks': (
            {
                'q': _t('Γιατί έπεσε ο τζίρος μου;', 'Why did my turnover drop?'),
                'route': '/tenant/sales',
                'circuit': 'sales-analytics',
                'steps': _tl(
                    [
                        'Άνοιξε την Ανάλυση Πωλήσεων για την περίοδο που σε ανησυχεί.',
                        'Σύγκρινε «Συνολικός Τζίρος» με «Πωληθείσα Ποσότητα»: αν έπεσε μόνο η αξία, το θέμα είναι τιμή ή μείγμα· αν έπεσε και η ποσότητα, είναι ζήτηση.',
                        'Κατέβα στα breakdowns ανά κατηγορία και είδος για να βρεις την πηγή.',
                        'Έλεγξε και ανά υποκατάστημα — συχνά φταίει ένα σημείο.',
                    ],
                    [
                        'Open Sales Analytics for the period that worries you.',
                        'Compare "Total Turnover" against "Units Sold": if only value fell, it is price or mix; if quantity fell too, it is demand.',
                        'Drill into the category and item breakdowns to find the source.',
                        'Check by branch as well — often a single site is responsible.',
                    ],
                ),
                'why': _t(
                    'Ο Πίνακας Διοίκησης δείχνει ΟΤΙ έπεσε. Η Ανάλυση Πωλήσεων δείχνει ΓΙΑΤΙ.',
                    'The Executive Dashboard shows THAT it fell. Sales Analytics shows WHY.',
                ),
            },
            {
                'q': _t('Ποια είδη πουλάνε περισσότερο;', 'Which items sell the most?'),
                'route': '/tenant/sales',
                'circuit': 'sales-analytics',
                'steps': _tl(
                    ['Ανάλυση Πωλήσεων → πίνακας κορυφαίων ειδών.', 'Ταξινόμησε κατά αξία ή κατά ποσότητα ανάλογα με το ερώτημα.'],
                    ['Sales Analytics → the top items table.', 'Sort by value or by quantity depending on what you are asking.'],
                ),
                'why': _t(
                    'Κορυφαία σε αξία και κορυφαία σε τεμάχια είναι συχνά τελείως διαφορετικές λίστες.',
                    'Top by value and top by units are often completely different lists.',
                ),
            },
            {
                'q': _t('Πόσο πουλάει το κάθε κατάστημα;', 'How much does each store sell?'),
                'route': '/tenant/comparisons/branch-vs-branch',
                'circuit': 'comparisons',
                'steps': _tl(
                    ['Συγκρίσεις → Κατάστημα με κατάστημα.', 'Όρισε την περίοδο και διάλεξε τα σημεία.'],
                    ['Comparisons → Branch vs branch.', 'Set the period and pick the sites.'],
                ),
                'why': _t(
                    'Η σύγκριση κρατά την ίδια περίοδο και για τα δύο σκέλη, κάτι που το χειροκίνητο φιλτράρισμα συχνά χαλάει.',
                    'The comparison keeps both sides on the same period, which manual filtering often gets wrong.',
                ),
            },
            {
                'q': _t('Πού βρίσκω ένα συγκεκριμένο παραστατικό;', 'Where do I find a specific document?'),
                'route': '/tenant/sales-documents',
                'circuit': 'sales-documents',
                'steps': _tl(
                    [
                        'Παραστατικά Πωλήσεων.',
                        'Άνοιξε τα φίλτρα και βάλε περίοδο που να περιέχει την ημερομηνία του.',
                        'Γράψε στην αναζήτηση τον αριθμό παραστατικού ή τον πελάτη.',
                        'Διπλό κλικ στη γραμμή για τις γραμμές ειδών.',
                    ],
                    [
                        'Sales Documents.',
                        'Open the filters and set a period that contains its date.',
                        'Type the document number or the customer into search.',
                        'Double-click the row for the item lines.',
                    ],
                ),
                'why': _t(
                    'Οι αναλύσεις δείχνουν αθροίσματα. Το παραστατικό υπάρχει μόνο στο κύκλωμα παραστατικών.',
                    'Analytics screens show totals. The document itself only exists in the document stream.',
                ),
            },
            {
                'q': _t('Πόσο είναι το καλάθι στο κατάστημα;', 'What is the in-store basket value?'),
                'route': '/tenant/pos',
                'circuit': 'pos',
                'steps': _tl(
                    ['Αναλύσεις → POS.', 'Δες «Μέση Αξία Απόδειξης» και «Μέσος Αριθμός Ειδών».'],
                    ['Analytics → POS.', 'Read "Average Receipt Value" and "Average Items per Receipt".'],
                ),
                'why': _t(
                    'Αυτά τα δύο μαζί εξηγούν σχεδόν κάθε μεταβολή λιανικής.',
                    'Those two together explain nearly every movement in retail.',
                ),
            },
        ),
    },
    {
        'id': 'profit',
        'title': _t('Κερδοφορία και τιμές', 'Profitability and pricing'),
        'icon': 'fe-percent',
        'tasks': (
            {
                'q': _t('Ποιο είναι το πραγματικό μου περιθώριο;', 'What is my real margin?'),
                'route': '/tenant/sales',
                'circuit': 'sales-analytics',
                'steps': _tl(
                    ['Ανάλυση Πωλήσεων → κάρτες «Μικτό Κέρδος» και «Περιθώριο %».'],
                    ['Sales Analytics → the "Gross Profit" and "Margin %" cards.'],
                ),
                'why': _t(
                    'Εδώ αφαιρείται το κόστος πωληθέντων. Το «Μικτό Κέρδος Περιόδου» του Dashboard αφαιρεί ΑΓΟΡΕΣ, '
                    'που είναι χρήσιμο για ταμειακή εικόνα αλλά όχι για κερδοφορία.',
                    'Here cost of goods sold is subtracted. The Dashboard\'s "Period Gross Profit" subtracts PURCHASES, '
                    'which is useful for a cash view but not for profitability.',
                ),
            },
            {
                'q': _t('Ποια είδη πουλάω πολύ φθηνά;', 'Which items am I selling too cheaply?'),
                'route': '/tenant/price-control',
                'circuit': 'price-control',
                'steps': _tl(
                    [
                        'Έλεγχος Τιμών.',
                        'Όρισε τον στόχο μεικτού περιθωρίου που θέλεις.',
                        'Πάτα «Κάτω από Προβλεπόμενη» για τη λίστα.',
                        'Ξεκίνα από τα είδη με μεγάλο τζίρο και μεγάλη απόκλιση.',
                    ],
                    [
                        'Price Control.',
                        'Set the target gross margin you want.',
                        'Click "Below Target" for the list.',
                        'Start with the high-turnover, high-deviation items.',
                    ],
                ),
                'why': _t(
                    'Ο Έλεγχος Τιμών συγκρίνει τη λιανική με μια τιμή στόχου που υπολογίζεται από την πραγματική τιμή κτήσης.',
                    'Price Control compares your retail price against a target derived from the real acquisition price.',
                ),
            },
            {
                'q': _t('Πόση έκπτωση παίρνω από τους προμηθευτές;', 'What discount am I getting from suppliers?'),
                'route': '/tenant/purchases',
                'circuit': 'purchases-analytics',
                'steps': _tl(
                    ['Ανάλυση Αγορών.', 'Σύγκρινε «Σύνολο Αγορών» με «Κόστος Αγορών» — η διαφορά είναι η έκπτωση.'],
                    ['Purchases Analytics.', 'Compare "Total Purchases" against "Purchase Cost" — the gap is the discount.'],
                ),
                'why': _t(
                    'Πιάνει μόνο εκπτώσεις γραμμής. Οι πιστώσεις τζίρου παρακολουθούνται στις Συμφωνίες Προμηθευτών.',
                    'It only captures line discounts. Volume rebates are tracked under Supplier Agreements.',
                ),
            },
            {
                'q': _t('Ποια είδη μου δεσμεύουν κεφάλαιο χωρίς να αποδίδουν;', 'Which items tie up capital without returning?'),
                'route': '/tenant/exports/sellout',
                'circuit': 'sellout',
                'steps': _tl(
                    [
                        'Sell Out.',
                        'Δες το GMROI ανά είδος — τιμή κάτω από 1 σημαίνει ότι το είδος δεν αποδίδει όσο κοστίζει να το κρατάς.',
                        'Διασταύρωσε με το Destocking Brief.',
                    ],
                    [
                        'Sell Out.',
                        'Read GMROI per item — below 1 means the item returns less than it costs to hold.',
                        'Cross-check against the Destocking Brief.',
                    ],
                ),
                'why': _t(
                    'Το GMROI συνδυάζει κέρδος και δεσμευμένο κεφάλαιο — μόνο το ένα από τα δύο παραπλανά.',
                    'GMROI combines profit with tied-up capital — either one on its own misleads.',
                ),
            },
        ),
    },
    {
        'id': 'stock',
        'title': _t('Απόθεμα και παραγγελίες', 'Stock and ordering'),
        'icon': 'fe-package',
        'tasks': (
            {
                'q': _t('Τι πρέπει να παραγγείλω;', 'What should I order?'),
                'route': '/tenant/fnr',
                'circuit': 'fnr',
                'steps': _tl(
                    [
                        'Πρώτα άνοιξε τις Παραγγελίες Προμηθευτών και δες τι εκκρεμεί ήδη.',
                        'Μετά πήγαινε στο FnR Worksheet.',
                        'Φίλτραρε σε προμηθευτή και σημείο, ρύθμισε target/overstock.',
                        'Έλεγξε «Γραμμές Παραγγελίας» και «Αξία Παραγγελίας» και κάνε εξαγωγή σε Excel.',
                    ],
                    [
                        'First open Supplier Orders and see what is already outstanding.',
                        'Then go to the FnR Worksheet.',
                        'Filter to a supplier and site, set target/overstock.',
                        'Check "Order Rows" and "Order Value", then export to Excel.',
                    ],
                ),
                'why': _t(
                    'Το BI δεν γνωρίζει backorders, οπότε ο έλεγχος ανοιχτών παραγγελιών πριν την πρόταση αποτρέπει διπλές παραγγελίες.',
                    'BI has no backorder concept, so checking open orders first is what prevents double ordering.',
                ),
            },
            {
                'q': _t('Πόσο απόθεμα έχω αυτή τη στιγμή;', 'How much stock do I have right now?'),
                'route': '/tenant/inventory',
                'circuit': 'inventory-analytics',
                'steps': _tl(
                    [
                        'Ανάλυση Αποθέματος.',
                        'Διάλεξε ημερομηνία snapshot.',
                        'Δες «Ποσότητα σε Απόθεμα» και αφαίρεσε τη «Δεσμευμένη Ποσότητα» για το πραγματικά διαθέσιμο.',
                    ],
                    [
                        'Inventory Analytics.',
                        'Pick a snapshot date.',
                        'Read "Quantity On Hand" and subtract "Reserved Quantity" for what is genuinely available.',
                    ],
                ),
                'why': _t(
                    'Το απόθεμα είναι φωτογραφία μιας ημερομηνίας, όχι άθροισμα περιόδου.',
                    'Stock is a point in time, not a period sum.',
                ),
            },
            {
                'q': _t('Ποια είδη κινδυνεύουν να μου τελειώσουν;', 'Which items am I about to run out of?'),
                'route': '/tenant/replenishment',
                'circuit': 'replenishment',
                'steps': _tl(
                    ['Replenishment.', 'Ταξινόμησε κατά «Εβδομάδες Κάλυψης» από τη μικρότερη.', 'Διασταύρωσε με τα αναμενόμενα από προμηθευτές.'],
                    ['Replenishment.', 'Sort by "Weeks of Stock", lowest first.', 'Cross-check against expected supplier receipts.'],
                ),
                'why': _t(
                    'Οι εβδομάδες κάλυψης συνδυάζουν απόθεμα με πραγματικό ρυθμό πώλησης — το σκέτο απόθεμα δεν λέει τίποτα.',
                    'Weeks of stock combine quantity with the real sales rate — a bare stock figure tells you nothing.',
                ),
            },
            {
                'q': _t('Πού πήγε το απόθεμα ενός είδους;', 'Where did an item\'s stock go?'),
                'route': '/tenant/warehouse-documents',
                'circuit': 'warehouse-documents',
                'steps': _tl(
                    ['Παραστατικά Αποθήκης.', 'Φίλτραρε ανά αποθήκη και περίοδο.', 'Ξεχώρισε τις μεταφορές από τις εισαγωγές/εξαγωγές.'],
                    ['Warehouse Documents.', 'Filter by warehouse and period.', 'Separate transfers from receipts and issues.'],
                ),
                'why': _t(
                    'Η μεταφορά δεν αλλάζει το συνολικό απόθεμα, μόνο την κατανομή του — γι\' αυτό «λείπει» από ένα σημείο.',
                    'A transfer does not change total stock, only its distribution — which is why it appears "missing" at one site.',
                ),
            },
            {
                'q': _t('Ποια είδη έχω παραπάνω απ\' όσο χρειάζομαι;', 'Which items do I hold more of than I need?'),
                'route': '/tenant/destocking',
                'circuit': 'destocking',
                'steps': _tl(
                    ['Destocking Brief.', 'Όρισε τις ημερομηνίες αποθέματος και το threshold.', 'Δες «Total Overstock» και τις προτάσεις.'],
                    ['Destocking Brief.', 'Set the stock dates and the threshold.', 'Read "Total Overstock" and the recommendations.'],
                ),
                'why': _t(
                    'Το Destocking κοιτάει και το περιθώριο, οπότε δεν προτείνει ξεφόρτωμα σε είδη που αποδίδουν.',
                    'Destocking also looks at margin, so it will not tell you to dump items that are performing.',
                ),
            },
        ),
    },
    {
        'id': 'money',
        'title': _t('Χρήματα και υπόλοιπα', 'Money and balances'),
        'icon': 'fe-credit-card',
        'tasks': (
            {
                'q': _t('Ποιος μου χρωστάει;', 'Who owes me money?'),
                'route': '/tenant/customers',
                'circuit': 'customer-balances',
                'steps': _tl(
                    ['Υπόλοιπα Πελατών.', 'Ταξινόμησε κατά ανοικτό υπόλοιπο.', 'Εστίασε στα ληξιπρόθεσμα και στις παλαιότερες ζώνες aging.'],
                    ['Customer Balances.', 'Sort by open balance.', 'Focus on overdue amounts and the oldest aging bands.'],
                ),
                'why': _t(
                    'Είναι τρέχον υπόλοιπο και δεν επηρεάζεται από την περίοδο — δεν χρειάζεται να ρυθμίσεις ημερομηνίες.',
                    'It is a current balance and the period does not affect it — no need to set any dates.',
                ),
            },
            {
                'q': _t('Σε ποιον χρωστάω;', 'Who do I owe?'),
                'route': '/tenant/suppliers',
                'circuit': 'supplier-balances',
                'steps': _tl(
                    ['Υπόλοιπα Προμηθευτών.', 'Ταξινόμησε κατά υπόλοιπο και δώσε προτεραιότητα στα ληξιπρόθεσμα.'],
                    ['Supplier Balances.', 'Sort by balance and prioritise the overdue amounts.'],
                ),
                'why': _t(
                    'Δείχνει και τη συγκέντρωση: αν λίγοι προμηθευτές κρατούν το μεγαλύτερο μέρος, έχεις διαπραγματευτική έκθεση.',
                    'It also shows concentration: if a few suppliers hold most of it, you have negotiating exposure.',
                ),
            },
            {
                'q': _t('Πού πήγαν τα λεφτά μου αυτόν τον μήνα;', 'Where did my money go this month?'),
                'route': '/tenant/cashflow',
                'circuit': 'cash-transactions',
                'steps': _tl(
                    ['Ταμειακές Ροές.', 'Διάλεξε κατηγορία και περίοδο.', 'Απόκλεισε τις μεταφορές μεταξύ λογαριασμών, δεν είναι δαπάνη.'],
                    ['Cash Flow.', 'Pick a category and period.', 'Exclude transfers between accounts, they are not spend.'],
                ),
                'why': _t(
                    'Ο τζίρος γράφεται με την πώληση, τα χρήματα κινούνται αλλού — μόνο οι ταμειακές ροές δείχνουν πραγματική ρευστότητα.',
                    'Turnover is booked at the sale, the money moves separately — only cash flow shows real liquidity.',
                ),
            },
            {
                'q': _t('Πόσα έξοδα έχω και πού πάνε;', 'What are my expenses and where do they go?'),
                'route': '/tenant/operating-expenses',
                'circuit': 'operating-expenses',
                'steps': _tl(
                    ['Λειτουργικά Έξοδα.', 'Δες το «Έξοδα / Έσοδα» και τα σύνολα ανά κατηγορία.', 'Σύγκρινε μήνα με μήνα για να βρεις τις αυξήσεις.'],
                    ['Operating Expenses.', 'Read "Expenses / Revenue" and the totals per category.', 'Compare month on month to find the increases.'],
                ),
                'why': _t(
                    'Τα λειτουργικά έξοδα είναι ξεχωριστό κύκλωμα από τις αγορές εμπορευμάτων — μην τα αθροίζεις.',
                    'Operating expenses are a separate stream from merchandise purchases — do not add them together.',
                ),
            },
        ),
    },
    {
        'id': 'admin',
        'title': _t('Λογαριασμός και δεδομένα', 'Account and data'),
        'icon': 'fe-settings',
        'tasks': (
            {
                'q': _t('Πώς προσθέτω χρήστη;', 'How do I add a user?'),
                'route': '/tenant/users',
                'circuit': 'tenant-users',
                'steps': _tl(
                    ['Προφίλ → Χρήστες.', 'Έλεγξε ότι έχεις διαθέσιμη άδεια.', 'Δημιούργησε τον χρήστη — θα λάβει email πρόσκλησης.'],
                    ['Profile → Users.', 'Check that you have a licence free.', 'Create the user — they will get an invitation email.'],
                ),
                'why': _t(
                    'Χρειάζεσαι ρόλο διαχειριστή tenant. Χωρίς διαθέσιμη άδεια η ενεργοποίηση αποτυγχάνει.',
                    'You need the tenant administrator role. Without a free licence, activation fails.',
                ),
            },
            {
                'q': _t('Πόσο πρόσφατα είναι τα δεδομένα που βλέπω;', 'How fresh is the data I am looking at?'),
                'route': '/tenant/dashboard',
                'circuit': 'dashboard',
                'steps': _tl(
                    ['Δες την ένδειξη «Τελευταίος συγχρονισμός» πάνω δεξιά σε κάθε σελίδα.'],
                    ['Check the "Last sync" indicator at the top right of every page.'],
                ),
                'why': _t(
                    'Κάθε αριθμός στο BI δείχνει μέχρι εκείνη τη στιγμή. Κινήσεις που έγιναν μετά δεν έχουν φτάσει ακόμη.',
                    'Every number in BI reflects data up to that moment. Anything booked after it has not arrived yet.',
                ),
            },
            {
                'q': _t('Πώς κατεβάζω τα δεδομένα σε Excel;', 'How do I export to Excel?'),
                'route': '/tenant/exports/reports',
                'circuit': 'exports',
                'steps': _tl(
                    [
                        'Όρισε πρώτα περίοδο και φίλτρα στη σελίδα που σε ενδιαφέρει.',
                        'Χρησιμοποίησε το κουμπί εξαγωγής της σελίδας, ή πήγαινε στις Εξαγωγές για έτοιμες αναφορές.',
                    ],
                    [
                        'Set the period and filters on the page you care about first.',
                        'Use that page\'s export button, or go to Exports for ready-made reports.',
                    ],
                ),
                'why': _t(
                    'Η εξαγωγή ακολουθεί τα ενεργά φίλτρα — αν δεν τα ορίσεις πρώτα, θα πάρεις άλλο εύρος από αυτό που περιμένεις.',
                    'The export follows the active filters — set them first or you will get a different range than you expect.',
                ),
            },
        ),
    },
)


# --------------------------------------------------------------------------
# FAQ / reconciliation playbooks
# --------------------------------------------------------------------------

FAQ: tuple[dict[str, Any], ...] = (
    {
        'id': 'mismatch',
        'q': _t('Ένα νούμερο δεν συμφωνεί με το SoftOne. Τι ελέγχω;', 'A number does not match SoftOne. What do I check?'),
        'steps': _tl(
            [
                'Περίοδος: έλεγξε ότι το «Από / Έως» καλύπτει ακριβώς το ίδιο διάστημα.',
                'Φίλτρα: υποκατάστημα, αποθηκευτικός χώρος, σειρά και κανάλι περιορίζουν σιωπηλά το αποτέλεσμα.',
                'Μέτρο: συγκρίνεις Καθαρή Αξία με Καθαρή Αξία; Το BI είναι καθαρό, το γενικό σύνολο του SoftOne περιέχει ΦΠΑ.',
                'Έξοδα: τα έξοδα παραστατικού μετράνε μία φορά ανά παραστατικό και δεν είναι είδος.',
                'Συμπεριφορές παραστατικών: μόνο όσες έχουν οριστεί ως πώληση/αγορά συμμετέχουν. Πιστωτικά και επιστροφές αφαιρούν.',
                'Συγχρονισμός: δες την ώρα τελευταίου συγχρονισμού πάνω δεξιά.',
            ],
            [
                'Period: confirm From / To covers exactly the same range.',
                'Filters: branch, warehouse, series and channel silently narrow the result.',
                'Measure: are you comparing Net Value against Net Value? BI is net; SoftOne\'s grand total includes VAT.',
                'Charges: document charges count once per document and are not an item.',
                'Document behaviours: only those configured as sale/purchase participate. Credit notes and returns subtract.',
                'Sync: check the last sync time at the top right.',
            ],
        ),
        'tip': _t(
            'Στις διαφορές συμφωνίας συγκρίνουμε πάντα ένα μέτρο τη φορά: Καθαρή Αξία, Έξοδα, ΦΠΑ και Σύνολο ξεχωριστά.',
            'When reconciling, always compare one measure at a time: Net Value, Charges, VAT and Total separately.',
        ),
    },
    {
        'id': 'zero',
        'q': _t('Μια σελίδα δείχνει μηδενικά ενώ ξέρω ότι υπάρχουν κινήσεις.', 'A page shows zeros although I know there is activity.'),
        'steps': _tl(
            [
                'Έλεγξε την περίοδο: η προεπιλογή είναι συχνά οι τελευταίες 30 ημέρες.',
                'Πάτα «Καθαρισμός» στα φίλτρα — μπορεί να έχει μείνει ενεργό φίλτρο από προηγούμενη χρήση.',
                'Αν η σελίδα δουλεύει με snapshot (απόθεμα), βεβαιώσου ότι η ημερομηνία snapshot έχει δεδομένα.',
                'Δες τον τελευταίο συγχρονισμό SoftOne.',
            ],
            [
                'Check the period: the default is often the last 30 days.',
                'Hit "Clear" on the filters — one may be left over from earlier.',
                'If the page works on a snapshot (stock), make sure that snapshot date has data.',
                'Check the last SoftOne sync.',
            ],
        ),
        'tip': _t(
            'Οι σελίδες αποθέματος χρειάζονται snapshot, όχι περίοδο. Ημερομηνία χωρίς snapshot δίνει άδεια οθόνη.',
            'Stock pages need a snapshot, not a period. A date with no snapshot gives you an empty screen.',
        ),
    },
    {
        'id': 'gross-profit',
        'q': _t('Γιατί το μικτό κέρδος διαφέρει ανάμεσα σε δύο σελίδες;', 'Why does gross profit differ between two pages?'),
        'steps': _tl(
            [
                'Στον Πίνακα Διοίκησης το «Μικτό Κέρδος Περιόδου» = Πωλήσεις - ΑΓΟΡΕΣ περιόδου.',
                'Στην Ανάλυση Πωλήσεων το «Μικτό Κέρδος» = Τζίρος - ΚΟΣΤΟΣ ΠΩΛΗΘΕΝΤΩΝ.',
                'Και τα δύο είναι σωστά, απαντούν σε διαφορετική ερώτηση: το πρώτο σε ταμειακή, το δεύτερο σε εμπορική.',
            ],
            [
                'On the Executive Dashboard, "Period Gross Profit" = Sales − period PURCHASES.',
                'In Sales Analytics, "Gross Profit" = Turnover − COST OF GOODS SOLD.',
                'Both are correct; they answer different questions — the first a cash one, the second a commercial one.',
            ],
        ),
        'tip': _t(
            'Για κερδοφορία χρησιμοποίησε πάντα το μικτό κέρδος της Ανάλυσης Πωλήσεων.',
            'For profitability, always use the gross profit from Sales Analytics.',
        ),
    },
    {
        'id': 'popup',
        'q': _t('Δεν ανοίγει ένα popup.', 'A popup will not open.'),
        'steps': _tl(
            [
                'Περίμενε να ολοκληρωθεί η φόρτωση της σελίδας.',
                'Πάτα στο εικονίδιο πληροφορίας ή μεγέθυνσης της κάρτας.',
                'Σε πίνακα, δοκίμασε διπλό κλικ στη γραμμή.',
                'Αν επιμένει, κάνε ανανέωση σελίδας και ανέφερέ το με τη σελίδα και το κουμπί.',
            ],
            [
                'Wait for the page to finish loading.',
                'Click the card\'s info or expand icon.',
                'In a table, try double-clicking the row.',
                'If it persists, refresh and report it with the page and the button.',
            ],
        ),
        'tip': None,
    },
    {
        'id': 'vat',
        'q': _t('Με ΦΠΑ ή χωρίς ΦΠΑ είναι τα νούμερα;', 'Are the numbers with or without VAT?'),
        'steps': _tl(
            [
                'Όλα τα KPI τζίρου, αγορών, κέρδους και περιθωρίου είναι ΚΑΘΑΡΑ, χωρίς ΦΠΑ.',
                'Ο ΦΠΑ εμφανίζεται μόνο στα κυκλώματα παραστατικών, ως ξεχωριστή στήλη.',
                'Το «Γενικό Σύνολο» των παραστατικών είναι το μόνο μέγεθος που περιέχει ΦΠΑ.',
                'Στον Έλεγχο Τιμών οι τιμές λιανικής και στόχου εμφανίζονται ΜΕ ΦΠΑ για να συγκρίνονται με το ράφι, αλλά το περιθώριο υπολογίζεται καθαρά.',
            ],
            [
                'Every turnover, purchase, profit and margin KPI is NET, excluding VAT.',
                'VAT appears only in the document streams, as its own column.',
                'The documents\' "Grand Total" is the only figure that includes VAT.',
                'In Price Control the retail and target prices are shown WITH VAT so they match the shelf, but the margin is computed net.',
            ],
        ),
        'tip': None,
    },
    {
        'id': 'access',
        'q': _t('Δεν βλέπω μια σελίδα που είχα πριν.', 'A page I used to have is missing.'),
        'steps': _tl(
            [
                'Το μενού διαμορφώνεται από το πακέτο συνδρομής και τον ρόλο σου.',
                'Ένα εικονίδιο κλειδαριάς δίπλα σε επιλογή σημαίνει ότι δεν περιλαμβάνεται στο πακέτο.',
                'Αν χρειάζεσαι πρόσβαση, μίλα με τον διαχειριστή tenant της εταιρείας σου.',
            ],
            [
                'The menu is shaped by your subscription package and your role.',
                'A padlock next to an entry means it is not included in the package.',
                'If you need access, talk to your company\'s tenant administrator.',
            ],
        ),
        'tip': None,
    },
)


# --------------------------------------------------------------------------
# Lookups used by the routes
# --------------------------------------------------------------------------

@lru_cache(maxsize=4)
def circuits_for_lang(lang: str = 'el') -> list[dict[str, Any]]:
    return [localize(c, lang) for c in CIRCUITS]


@lru_cache(maxsize=4)
def circuits_by_id_for_lang(lang: str = 'el') -> dict[str, dict[str, Any]]:
    return {c['id']: c for c in circuits_for_lang(lang)}


def circuit_for_lang(circuit_id: str, lang: str = 'el') -> dict[str, Any] | None:
    return circuits_by_id_for_lang(lang).get(circuit_id)


@lru_cache(maxsize=4)
def task_groups_for_lang(lang: str = 'el') -> list[dict[str, Any]]:
    return [
        {**g, 'title': localize(g['title'], lang), 'tasks': [localize(t, lang) for t in g['tasks']]}
        for g in TASK_GROUPS
    ]


@lru_cache(maxsize=4)
def faq_for_lang(lang: str = 'el') -> list[dict[str, Any]]:
    return [localize(f, lang) for f in FAQ]


def kpis_for_circuit(circuit_id: str, lang: str = 'el') -> list[dict[str, Any]]:
    """KPIs shown in a circuit's manual page.

    A KPI belongs to exactly one circuit in the catalog (its "home"), but several
    screens legitimately show the same number — the finance dashboard repeats the
    receivables cards, every document stream repeats the footer totals. `also_kpis`
    on a circuit pulls those in without duplicating any text.
    """
    from app.core.kpi_catalog import catalog_by_circuit, catalog_for_lang

    own = catalog_by_circuit(lang).get(circuit_id, [])
    circuit = CIRCUITS_BY_ID.get(circuit_id) or {}
    extra_ids = list(circuit.get('also_kpis') or [])
    if not extra_ids:
        return list(own)
    by_id = {entry['id']: entry for entry in catalog_for_lang(lang)}
    seen = {entry['id'] for entry in own}
    merged = list(own)
    for kpi_id in extra_ids:
        if kpi_id in seen:
            continue
        entry = by_id.get(kpi_id)
        if entry is not None:
            merged.append(entry)
            seen.add(kpi_id)
    return merged


@lru_cache(maxsize=4)
def circuit_groups(lang: str = 'el') -> list[tuple[str, list[dict[str, Any]]]]:
    """Circuits bucketed by their sidebar group prefix, for the manual's index."""
    order: list[str] = []
    buckets: dict[str, list[dict[str, Any]]] = {}
    for circuit in circuits_for_lang(lang):
        head = str(circuit.get('group') or '').split('→')[0].strip() or 'Other'
        if head not in buckets:
            buckets[head] = []
            order.append(head)
        buckets[head].append(circuit)
    return [(name, buckets[name]) for name in order]
