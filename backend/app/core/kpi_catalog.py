"""Single source of truth for KPI help.

Every KPI the tenant portal shows is described exactly once, here. The same
entries feed:

  * the KPI help popup (base_tenant.html serialises this catalog to JS and
    matches a clicked card against it by title),
  * the KPI dictionary page of the in-app manual (/tenant/help/kpis),
  * the per-circuit KPI tables of the manual (/tenant/help/circuits).

Before this module the popup text lived in a JS dictionary inside
base_tenant.html and the manual repeated the same explanations in a Jinja
literal, so the two drifted apart. Add a KPI here and it shows up in all three
places at once.

Each entry carries more than "what + formula": the *source* tells the user which
stream the number is read from, *filters* which controls move it, and *caveats*
the traps that make a number look wrong when it is not (VAT, document
behaviours, snapshot vs period, sync lag). Those three fields are what turns a
number into something a user can reconcile against SoftOne on their own.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

Lang = str

#  Manual anchors these KPIs belong to; kept in sync with help_content.CIRCUITS.
_ANCHOR_FALLBACK = 'quick-start'


@dataclass(frozen=True)
class Kpi:
    """One KPI, described in both languages."""

    id: str
    #  Titles as they are actually rendered on the cards, plus common aliases.
    #  Matching is accent/case insensitive (see normalize_kpi_key).
    keys: tuple[str, ...]
    title: dict[Lang, str]
    what: dict[Lang, str]
    formula: dict[Lang, str]
    #  Which stream/table the number is read from.
    source: dict[Lang, str] = field(default_factory=dict)
    #  Which controls change it.
    filters: dict[Lang, str] = field(default_factory=dict)
    #  What it does NOT include / how to not misread it.
    caveats: dict[Lang, str] = field(default_factory=dict)
    #  Anchor in /tenant/help/circuits.
    circuit: str = _ANCHOR_FALLBACK

    def as_dict(self, lang: Lang) -> dict[str, Any]:
        def pick(mapping: dict[Lang, str]) -> str:
            if not mapping:
                return ''
            return mapping.get(lang) or mapping.get('el') or ''

        return {
            'id': self.id,
            #  Aliases collapse once normalised ('Περιθώριο' / 'Περιθώριο %'), so
            #  dedupe to keep the payload small while preserving order.
            'keys': list(dict.fromkeys(normalize_kpi_key(k) for k in self.keys if normalize_kpi_key(k))),
            'title': pick(self.title),
            'what': pick(self.what),
            'formula': pick(self.formula),
            'source': pick(self.source),
            'filters': pick(self.filters),
            'caveats': pick(self.caveats),
            'circuit': self.circuit,
        }


def normalize_kpi_key(raw: str | None) -> str:
    """Lower-case, strip accents and punctuation so 'Τζίρος Ημέρας' matches
    'τζιρος ημερας'. Mirrored byte-for-byte by normalizeKpiKey() in JS."""
    if not raw:
        return ''
    text = unicodedata.normalize('NFD', str(raw))
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn')
    text = text.lower()
    out = []
    for ch in text:
        out.append(ch if (ch.isalnum() or ch.isspace()) else ' ')
    return ' '.join(''.join(out).split())


# --- shared boilerplate -----------------------------------------------------

_PERIOD_FILTERS = {
    'el': 'Περίοδος «Από / Έως», υποκατάστημα, αποθηκευτικός χώρος, κανάλι, σειρά και όποιο άλλο φίλτρο είναι ενεργό στη σελίδα.',
    'en': 'The From / To period, branch, warehouse, channel, series and any other active page filter.',
}
_SNAPSHOT_FILTERS = {
    'el': 'Ημερομηνία snapshot, αποθηκευτικός χώρος, υποκατάστημα, κατηγορία, brand και ομάδα.',
    'en': 'Snapshot date, warehouse, branch, category, brand and group.',
}
_VAT_CAVEAT = {
    'el': 'Είναι καθαρή αξία, χωρίς ΦΠΑ. Αν το συγκρίνεις με σύνολο παραστατικού στο SoftOne θα βρεις διαφορά ίση με τον ΦΠΑ.',
    'en': 'This is a net amount, excluding VAT. Comparing it against a SoftOne document total will differ by exactly the VAT.',
}
_SYNC_CAVEAT = {
    'el': 'Δείχνει ό,τι έχει φτάσει μέχρι τον τελευταίο συγχρονισμό SoftOne (πάνω δεξιά). Κινήσεις μετά από αυτόν δεν μετράνε ακόμη.',
    'en': 'Reflects data up to the last SoftOne sync (shown top right). Movements after that are not counted yet.',
}


def _m(el: str, en: str) -> dict[Lang, str]:
    return {'el': el, 'en': en}


# --- the catalog ------------------------------------------------------------

CATALOG: tuple[Kpi, ...] = (
    # ======================= Πωλήσεις / τζίρος =============================
    Kpi(
        id='sales_period',
        keys=('Πωλήσεις περιόδου', 'Πωλήσεις Περιόδου', 'Period sales', 'Sales'),
        title=_m('Πωλήσεις Περιόδου', 'Period Sales'),
        what=_m(
            'Συνολικός τζίρος πωλήσεων για το επιλεγμένο διάστημα και τα ενεργά φίλτρα, χωρίς ΦΠΑ και μαζί με τα έξοδα των παραστατικών.',
            'Total sales turnover for the selected period and active filters, excluding VAT and including document charges.',
        ),
        formula=_m(
            'Πωλήσεις Περιόδου = καθαρή αξία γραμμών + καθαρά έξοδα παραστατικού, μία φορά ανά παραστατικό, στο [Από, Έως].',
            'Period Sales = net line value + net document charges, counted once per document, within [From, To].',
        ),
        source=_m(
            'Κύκλωμα Πωλήσεων (fact_sales και τα ημερήσια aggregates agg_sales_daily).',
            'Sales stream (fact_sales and the agg_sales_daily aggregates).',
        ),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Χωρίς ΦΠΑ. Συμμετέχουν μόνο οι συμπεριφορές παραστατικών που έχει ορίσει το tenant ως «πώληση» — πιστωτικά και επιστροφές αφαιρούν. Τα έξοδα παραστατικού μετράνε μία φορά ανά παραστατικό, όχι ανά γραμμή.',
            'Excludes VAT. Only the document behaviours the tenant marks as "sale" participate — credit notes and returns subtract. Document charges count once per document, not per line.',
        ),
        circuit='sales-documents',
    ),
    Kpi(
        id='total_turnover',
        keys=('Συνολικός Τζίρος', 'Σύνολο Τζίρου', 'Total turnover'),
        title=_m('Συνολικός Τζίρος', 'Total Turnover'),
        what=_m(
            'Συνολική αξία πωλήσεων χωρίς ΦΠΑ, μαζί με τα καθαρά έξοδα του παραστατικού.',
            'Total sales value excluding VAT, including net document charges.',
        ),
        formula=_m(
            'Σύνολο Τζίρου = καθαρή αξία γραμμών + καθαρή αξία εξόδων παραστατικού, μία φορά ανά παραστατικό, στο [Από, Έως].',
            'Total Turnover = net line value + net document charges, once per document, within [From, To].',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_PERIOD_FILTERS,
        caveats=_VAT_CAVEAT,
        circuit='sales-analytics',
    ),
    Kpi(
        id='turnover_day',
        keys=('Τζίρος Ημέρας', 'Day turnover', 'Daily turnover'),
        title=_m('Τζίρος Ημέρας', 'Day Turnover'),
        what=_m(
            'Τζίρος πωλήσεων μόνο της ημέρας αναφοράς, χωρίς ΦΠΑ και με τα έξοδα παραστατικών.',
            'Sales turnover for the reference day only, excluding VAT and including document charges.',
        ),
        formula=_m(
            'Τζίρος Ημέρας = καθαρή αξία γραμμών + καθαρά έξοδα παραστατικού, μία φορά ανά παραστατικό, της ίδιας ημέρας.',
            'Day Turnover = net line value + net document charges, once per document, for that single day.',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_m(
            'Η ημέρα αναφοράς είναι η ημερομηνία «Έως» του φίλτρου, όχι απαραίτητα το σήμερα.',
            'The reference day is the "To" date of the filter, not necessarily today.',
        ),
        caveats=_m(
            'Αν αλλάξεις το «Έως», αλλάζει και η ημέρα που μετράει. Δείχνει 0 όταν η ημέρα αυτή δεν έχει κινήσεις ή δεν έχει συγχρονιστεί ακόμη.',
            'Changing "To" changes which day is measured. Shows 0 when that day has no movements or has not synced yet.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='turnover_week',
        keys=('Τζίρος Εβδομάδας', 'Week turnover'),
        title=_m('Τζίρος Εβδομάδας', 'Week Turnover'),
        what=_m(
            'Τζίρος πωλήσεων από την αρχή της εβδομάδας μέχρι την ημερομηνία αναφοράς.',
            'Sales turnover from the start of the week up to the reference date.',
        ),
        formula=_m(
            'Τζίρος Εβδομάδας = καθαρή αξία γραμμών + καθαρά έξοδα παραστατικού από τη Δευτέρα της εβδομάδας έως την ημερομηνία «Έως».',
            'Week Turnover = net line value + net document charges from that week\'s Monday through the "To" date.',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_m(
            'Η εβδομάδα ξεκινά Δευτέρα και κλείνει στην ημερομηνία «Έως».',
            'The week starts on Monday and ends at the "To" date.',
        ),
        caveats=_m(
            'Είναι μερική εβδομάδα όταν το «Έως» δεν είναι Κυριακή. Μη το συγκρίνεις με ολόκληρη εβδομάδα.',
            'It is a partial week whenever "To" is not a Sunday. Do not compare it against a full week.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='turnover_month',
        keys=('Τζίρος Μήνα', 'Month turnover'),
        title=_m('Τζίρος Μήνα', 'Month Turnover'),
        what=_m(
            'Τζίρος πωλήσεων από την 1η του μήνα μέχρι την ημερομηνία αναφοράς.',
            'Sales turnover from the 1st of the month up to the reference date.',
        ),
        formula=_m(
            'Τζίρος Μήνα = καθαρή αξία γραμμών + καθαρά έξοδα παραστατικού από την 1η του μήνα έως την ημερομηνία «Έως».',
            'Month Turnover = net line value + net document charges from the 1st of the month through the "To" date.',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_m('Ο μήνας προκύπτει από την ημερομηνία «Έως».', 'The month is derived from the "To" date.'),
        caveats=_m(
            'Μερικός μήνας όταν το «Έως» δεν είναι η τελευταία ημέρα του μήνα.',
            'A partial month whenever "To" is not the last day of the month.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='turnover_ytd',
        keys=(
            'Τζίρος YTD', 'Τζίρος Τρέχοντος Έτους', 'Τζίρος 2026 (YTD)', 'Τζίρος 2025 (YTD)',
            'Current year turnover', 'YTD turnover',
        ),
        title=_m('Τζίρος Έτους (YTD)', 'Year Turnover (YTD)'),
        what=_m(
            'Ο τζίρος από 01/01 του έτους μέχρι την ημερομηνία «Έως», με σύγκριση απέναντι στο προηγούμενο έτος.',
            'Turnover from 01/01 of the year through the "To" date, compared against the previous year.',
        ),
        formula=_m(
            'Τζίρος YTD = άθροισμα καθαρής αξίας + καθαρών εξόδων παραστατικού από 01/01 έως «Έως». Σύγκριση = ((YTD - Προηγ. Έτος) / Προηγ. Έτος) * 100.',
            'YTD Turnover = sum of net value + net document charges from 01/01 through "To". Comparison = ((YTD - Prev. Year) / Prev. Year) * 100.',
        ),
        source=_m('Κύκλωμα Πωλήσεων (μηνιαία aggregates).', 'Sales stream (monthly aggregates).'),
        filters=_m(
            'Το έτος και το cutoff προκύπτουν από την ημερομηνία «Έως». Τα υπόλοιπα φίλτρα σελίδας ισχύουν κανονικά.',
            'Year and cutoff come from the "To" date. All other page filters still apply.',
        ),
        caveats=_m(
            'Η κάρτα του προηγούμενου έτους υπάρχει σε δύο εκδοχές: (YTD) που κόβει στην ίδια ημέρα και (FULL YEAR) που είναι ολόκληρο το έτος. Μην τις μπερδέψεις σε σύγκριση.',
            'The previous-year card comes in two flavours: (YTD) cut at the same day, and (FULL YEAR) for the whole year. Do not mix them up when comparing.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='turnover_prev_full',
        keys=('Τζίρος 2025 (FULL YEAR)', 'Τζίρος Προηγ. Έτους Full', 'Previous year full'),
        title=_m('Τζίρος Προηγούμενου Έτους (Full Year)', 'Previous Year Turnover (Full Year)'),
        what=_m(
            'Ο συνολικός τζίρος ολόκληρου του προηγούμενου έτους, ως βάση σύγκρισης.',
            'Total turnover for the entire previous year, used as a comparison base.',
        ),
        formula=_m(
            'Τζίρος Προηγ. Έτους Full = άθροισμα καθαρής αξίας + καθαρών εξόδων από 01/01 έως 31/12 του προηγούμενου έτους.',
            'Prev. Year Full = sum of net value + net charges from 01/01 to 31/12 of the previous year.',
        ),
        source=_m('Κύκλωμα Πωλήσεων (μηνιαία aggregates).', 'Sales stream (monthly aggregates).'),
        filters=_m(
            'Δεν επηρεάζεται από το «Από/Έως» — είναι πάντα ολόκληρο το προηγούμενο έτος.',
            'Not affected by From/To — it is always the full previous year.',
        ),
        caveats=_m(
            'Επειδή είναι ολόκληρο έτος, η σύγκρισή του με φετινό YTD θα δείχνει πάντα υστέρηση μέχρι το τέλος της χρονιάς. Είναι δείκτης προόδου, όχι απόδοσης.',
            'Being a full year, comparing it to a current YTD always looks behind until year end. It is a progress gauge, not a performance one.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='turnover_ytd_vs_full',
        keys=('Τζίρος 2026 (YTD vs FULL)', 'YTD vs Full'),
        title=_m('Τζίρος YTD vs Full Έτους', 'Turnover YTD vs Full Year'),
        what=_m(
            'Πόσο έχει καλύψει ο φετινός τζίρος μέχρι σήμερα απέναντι σε ολόκληρο το προηγούμενο έτος.',
            'How much of last year\'s full turnover this year has covered so far.',
        ),
        formula=_m(
            'Σύγκριση = ((Τζίρος Τρέχοντος Έτους YTD - Τζίρος Προηγ. Έτους Full) / Τζίρος Προηγ. Έτους Full) * 100.',
            'Comparison = ((Current Year YTD - Prev. Year Full) / Prev. Year Full) * 100.',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_m('Το cutoff έρχεται από την ημερομηνία «Έως».', 'The cutoff comes from the "To" date.'),
        caveats=_m(
            'Αρνητικό ποσοστό στη μέση της χρονιάς είναι φυσιολογικό: συγκρίνεις μερικό έτος με πλήρες.',
            'A negative percentage mid-year is normal: you are comparing a partial year against a full one.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='avg_daily_turnover',
        keys=('Μέσος ημερήσιος τζίρος', 'Μέσος Ημερήσιος Τζίρος', 'Μ.Ο. Πώληση / Ημέρα', 'Avg daily turnover'),
        title=_m('Μέσος Ημερήσιος Τζίρος', 'Average Daily Turnover'),
        what=_m(
            'Μέσος καθαρός τζίρος ανά ημέρα στο διάστημα που έχεις επιλέξει.',
            'Average net turnover per day over the selected period.',
        ),
        formula=_m(
            'Μέσος Ημερήσιος Τζίρος = Πωλήσεις Περιόδου / πλήθος ημερών από «Από» έως «Έως» (συμπεριλαμβανομένων).',
            'Average Daily Turnover = Period Sales / number of days from "From" to "To" (inclusive).',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Ο παρονομαστής είναι ημερολογιακές ημέρες, όχι ημέρες λειτουργίας. Αν το κατάστημα είναι κλειστό Κυριακές, ο μέσος όρος βγαίνει χαμηλότερος από τον πραγματικό ημερήσιο ρυθμό.',
            'The denominator is calendar days, not trading days. If you close on Sundays the average reads lower than your real trading-day rate.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='qty_sold',
        keys=('Πωληθείσα Ποσότητα', 'Ποσότητα μήνα', 'Ποσότητα Μήνα', 'Ποσότητα', 'Units sold', 'Quantity sold'),
        title=_m('Πωληθείσα Ποσότητα', 'Units Sold'),
        what=_m(
            'Συνολικές μονάδες που πουλήθηκαν στο επιλεγμένο διάστημα.',
            'Total units sold within the selected period.',
        ),
        formula=_m(
            'Πωληθείσα Ποσότητα = άθροισμα qty όλων των γραμμών πώλησης στο [Από, Έως].',
            'Units Sold = sum of qty across all sales lines within [From, To].',
        ),
        source=_m('Κύκλωμα Πωλήσεων (γραμμές παραστατικών).', 'Sales stream (document lines).'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Οι επιστροφές αφαιρούν ποσότητα. Δωρεάν τεμάχια μετράνε στην ποσότητα αλλά όχι στην αξία, οπότε ποσότητα και τζίρος δεν κινούνται πάντα μαζί.',
            'Returns subtract quantity. Free goods count in quantity but not in value, so units and turnover do not always move together.',
        ),
        circuit='sales-analytics',
    ),
    Kpi(
        id='growth_vs_prev',
        keys=('Μεταβολή vs Προηγ.', 'Growth vs prev', 'Μεταβολή'),
        title=_m('Μεταβολή vs Προηγούμενη Περίοδο', 'Growth vs Previous Period'),
        what=_m(
            'Η μεταβολή του τζίρου σε σχέση με την αμέσως προηγούμενη συγκρίσιμη περίοδο.',
            'Turnover change against the immediately preceding comparable period.',
        ),
        formula=_m(
            'Μεταβολή = ((Τζίρος Τρέχουσας Περιόδου - Τζίρος Προηγούμενης Περιόδου) / Τζίρος Προηγούμενης Περιόδου) * 100.',
            'Growth = ((Current Period Turnover - Previous Period Turnover) / Previous Period Turnover) * 100.',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_m(
            'Η προηγούμενη περίοδος έχει το ίδιο μήκος με την τρέχουσα και τοποθετείται αμέσως πριν από αυτήν.',
            'The previous period has the same length as the current one and sits immediately before it.',
        ),
        caveats=_m(
            'Όταν η προηγούμενη περίοδος είναι μηδενική, το ποσοστό δεν ορίζεται και εμφανίζεται παύλα. Προσοχή στην εποχικότητα: μήνας-με-μήνα δεν είναι πάντα συγκρίσιμος.',
            'When the previous period is zero the percentage is undefined and shows a dash. Mind seasonality: month-over-month is not always comparable.',
        ),
        circuit='sales-analytics',
    ),

    # ======================= Κερδοφορία ====================================
    Kpi(
        id='gross_profit',
        keys=('Μικτό Κέρδος', 'Gross profit'),
        title=_m('Μικτό Κέρδος', 'Gross Profit'),
        what=_m(
            'Μικτό αποτέλεσμα πωλήσεων στο επιλεγμένο διάστημα.',
            'Gross sales result for the selected period.',
        ),
        formula=_m(
            'Μικτό Κέρδος = Τζίρος (καθαρή αξία + έξοδα παραστατικού) - Κόστος Πωληθέντων, όπου το κόστος πωληθέντων είναι το άθροισμα cost_amount των γραμμών πώλησης.',
            'Gross Profit = Turnover (net value + document charges) - COGS, where COGS is the sum of cost_amount over sales lines.',
        ),
        source=_m(
            'Κύκλωμα Πωλήσεων· το κόστος έρχεται από το cost_amount της γραμμής, όπως το δίνει το SoftOne.',
            'Sales stream; cost comes from the line cost_amount as provided by SoftOne.',
        ),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Αν το SoftOne δεν έχει αποτιμήσει κόστος σε κάποιες γραμμές, το μικτό κέρδος βγαίνει τεχνητά υψηλό. Δεν είναι λογιστικό αποτέλεσμα — δεν περιλαμβάνει λειτουργικά έξοδα.',
            'If SoftOne has not costed some lines, gross profit reads artificially high. This is not an accounting result — operating expenses are not included.',
        ),
        circuit='sales-analytics',
    ),
    Kpi(
        id='gross_profit_period',
        keys=('Μικτό Κέρδος περιόδου', 'Μικτό Κέρδος Περιόδου'),
        title=_m('Μικτό Κέρδος Περιόδου', 'Period Gross Profit'),
        what=_m(
            'Το αποτέλεσμα πωλήσεων μείον αγορές για το επιλεγμένο διάστημα.',
            'Sales minus purchases for the selected period.',
        ),
        formula=_m(
            'Μικτό Κέρδος Περιόδου = Πωλήσεις Περιόδου - Αγορές Περιόδου.',
            'Period Gross Profit = Period Sales - Period Purchases.',
        ),
        source=_m('Κυκλώματα Πωλήσεων και Αγορών.', 'Sales and Purchases streams.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Προσοχή: εδώ αφαιρούνται οι ΑΓΟΡΕΣ της περιόδου, όχι το κόστος πωληθέντων. Σε μήνα με μεγάλη παραγγελία το νούμερο πέφτει χωρίς να έχει χειροτερέψει η κερδοφορία. Για πραγματικό περιθώριο δες το «Μικτό Κέρδος» των Αναλύσεων Πωλήσεων.',
            'Careful: this subtracts period PURCHASES, not cost of goods sold. In a month with a large order it drops without profitability worsening. For true margin use "Gross Profit" in Sales Analytics.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='gross_profit_year',
        keys=('Μικτό Κέρδος έτους', 'Μικτό Κέρδος Έτους', 'Year gross profit'),
        title=_m('Μικτό Κέρδος Έτους', 'Year Gross Profit'),
        what=_m(
            'Μικτό αποτέλεσμα από την αρχή του έτους μέχρι την ημερομηνία αναφοράς.',
            'Gross result from year start through the reference date.',
        ),
        formula=_m(
            'Μικτό Κέρδος Έτους = Τζίρος Τρέχοντος Έτους YTD - Αγορές Τρέχοντος Έτους YTD.',
            'Year Gross Profit = Current Year YTD Turnover - Current Year YTD Purchases.',
        ),
        source=_m('Κυκλώματα Πωλήσεων και Αγορών.', 'Sales and Purchases streams.'),
        filters=_m('Το cutoff έρχεται από την ημερομηνία «Έως».', 'The cutoff comes from the "To" date.'),
        caveats=_m(
            'Όπως και στο μικτό κέρδος περιόδου, αφαιρούνται αγορές και όχι κόστος πωληθέντων.',
            'As with period gross profit, purchases are subtracted rather than cost of goods sold.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='margin_pct',
        keys=('Περιθώριο', 'Περιθώριο %', 'Μέσο Περιθώριο %', 'Μικτό %', 'Margin %', 'Margin'),
        title=_m('Περιθώριο %', 'Margin %'),
        what=_m(
            'Το ποσοστό κερδοφορίας πάνω στις πωλήσεις — πόσα σεντ κέρδους μένουν σε κάθε ευρώ τζίρου.',
            'Profitability as a percentage of sales — how much of every euro of turnover is profit.',
        ),
        formula=_m(
            'Περιθώριο % = ((Τζίρος - Κόστος) / Τζίρος) * 100.',
            'Margin % = ((Turnover - Cost) / Turnover) * 100.',
        ),
        source=_m('Κύκλωμα Πωλήσεων.', 'Sales stream.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Είναι περιθώριο πάνω στην τιμή πώλησης, όχι markup πάνω στο κόστος: κόστος 100 και τιμή 125 δίνει περιθώριο 20%, όχι 25%. Στο Executive Dashboard το «Μέσο Περιθώριο %» υπολογίζεται με βάση τις αγορές της περιόδου.',
            'This is margin on the selling price, not markup on cost: cost 100 sold at 125 is a 20% margin, not 25%. On the Executive Dashboard "Average Margin %" is derived from period purchases.',
        ),
        circuit='sales-analytics',
    ),
    Kpi(
        id='avg_margin_per_branch',
        keys=('Μ.Ο. Περιθώριο / Κατάστημα', 'Avg margin per branch'),
        title=_m('Μ.Ο. Περιθώριο ανά Κατάστημα', 'Average Margin per Branch'),
        what=_m(
            'Μέσο περιθώριο αν δεις κάθε υποκατάστημα ξεχωριστά και μετά πάρεις τον μέσο όρο.',
            'Average margin computed per branch and then averaged across branches.',
        ),
        formula=_m(
            'Μ.Ο. Περιθώριο / Κατάστημα = μέσος όρος των ποσοστών ((τζίρος branch - κόστος branch) / τζίρος branch) * 100 για όλα τα υποκαταστήματα.',
            'Avg Margin / Branch = mean of ((branch turnover - branch cost) / branch turnover) * 100 across all branches.',
        ),
        source=_m('Κύκλωμα Πωλήσεων ανά υποκατάστημα.', 'Sales stream by branch.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Είναι απλός μέσος όρος, όχι σταθμισμένος: ένα μικρό κατάστημα βαραίνει όσο και ένα μεγάλο. Διαφέρει από το συνολικό περιθώριο της εταιρείας.',
            'It is an unweighted mean: a small branch counts as much as a large one. It differs from company-wide margin.',
        ),
        circuit='sales-analytics',
    ),

    # ======================= Αγορές ========================================
    Kpi(
        id='purchases_period',
        keys=('Αγορές περιόδου', 'Αγορές Περιόδου', 'Period purchases'),
        title=_m('Αγορές Περιόδου', 'Period Purchases'),
        what=_m(
            'Συνολική αξία αγορών για το επιλεγμένο διάστημα και τα φίλτρα, χωρίς ΦΠΑ και με έξοδα παραστατικών.',
            'Total purchase value for the selected period and filters, excluding VAT and including document charges.',
        ),
        formula=_m(
            'Αγορές Περιόδου = καθαρή αξία γραμμών αγοράς + καθαρά έξοδα παραστατικού αγοράς, μία φορά ανά παραστατικό, στο [Από, Έως].',
            'Period Purchases = net purchase line value + net purchase document charges, once per document, within [From, To].',
        ),
        source=_m('Κύκλωμα Αγορών (fact_purchases και τα ημερήσια aggregates).', 'Purchases stream (fact_purchases and daily aggregates).'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Μετράει την ημερομηνία του παραστατικού αγοράς, όχι την ημερομηνία παραγγελίας ή παραλαβής. Τα πιστωτικά προμηθευτή αφαιρούν.',
            'Uses the purchase document date, not the order or receipt date. Supplier credit notes subtract.',
        ),
        circuit='purchase-documents',
    ),
    Kpi(
        id='purchases_total',
        keys=('Σύνολο Αγορών', 'Total purchases'),
        title=_m('Σύνολο Αγορών', 'Total Purchases'),
        what=_m(
            'Η αξία αγορών πριν αφαιρεθούν οι εκπτώσεις γραμμής — δηλαδή η τιμή τιμοκαταλόγου.',
            'Purchase value before line discounts — i.e. list price value.',
        ),
        formula=_m(
            'Σύνολο Αγορών = καθαρή αξία μετά εκπτώσεων + εκπτώσεις γραμμών αγορών, στο [Από, Έως].',
            'Total Purchases = net value after discounts + purchase line discounts, within [From, To].',
        ),
        source=_m('Κύκλωμα Αγορών.', 'Purchases stream.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Είναι μεγαλύτερο από το «Κόστος Αγορών» ακριβώς κατά το ποσό των εκπτώσεων. Χρησιμοποίησέ το μόνο για να μετρήσεις πόση έκπτωση πέτυχες.',
            'It exceeds "Purchase Cost" by exactly the discount amount. Use it only to measure how much discount you achieved.',
        ),
        circuit='purchases-analytics',
    ),
    Kpi(
        id='purchase_cost',
        keys=('Κόστος Αγορών', 'Purchase cost'),
        title=_m('Κόστος Αγορών', 'Purchase Cost'),
        what=_m(
            'Η καθαρή αξία αγορών μετά τις εκπτώσεις γραμμής — αυτό που πραγματικά πλήρωσες.',
            'Net purchase value after line discounts — what you actually paid.',
        ),
        formula=_m(
            'Κόστος Αγορών = άθροισμα net_value των παραστατικών αγορών στο [Από, Έως].',
            'Purchase Cost = sum of net_value over purchase documents within [From, To].',
        ),
        source=_m('Κύκλωμα Αγορών.', 'Purchases stream.'),
        filters=_PERIOD_FILTERS,
        caveats=_VAT_CAVEAT,
        circuit='purchases-analytics',
    ),
    Kpi(
        id='qty_purchased',
        keys=('Αγορασμένη Ποσότητα', 'Αγορασθείσα Ποσότητα', 'Units purchased'),
        title=_m('Αγορασμένη Ποσότητα', 'Units Purchased'),
        what=_m('Οι συνολικές μονάδες που αγοράστηκαν στο επιλεγμένο διάστημα.', 'Total units purchased in the selected period.'),
        formula=_m(
            'Αγορασμένη Ποσότητα = άθροισμα qty όλων των γραμμών αγορών στο [Από, Έως].',
            'Units Purchased = sum of qty across purchase lines within [From, To].',
        ),
        source=_m('Κύκλωμα Αγορών.', 'Purchases stream.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Τα δωρεάν τεμάχια προσμετρώνται στην ποσότητα με μηδενική αξία, οπότε ρίχνουν τη μέση τιμή κτήσης.',
            'Free goods count in quantity at zero value, so they lower the average acquisition price.',
        ),
        circuit='purchases-analytics',
    ),
    Kpi(
        id='purchase_discount_pct',
        keys=('Περιθώριο % αγορών', 'Έκπτωση %', 'Discount %'),
        title=_m('Έκπτωση Αγορών %', 'Purchase Discount %'),
        what=_m(
            'Το ποσοστό έκπτωσης που πέτυχαν οι αγορές στο επιλεγμένο διάστημα.',
            'The discount percentage achieved on purchases in the selected period.',
        ),
        formula=_m(
            'Έκπτωση % = ((Αξία πριν εκπτώσεις - Αξία μετά εκπτώσεις) / Αξία πριν εκπτώσεις) * 100.',
            'Discount % = ((Value before discounts - Value after discounts) / Value before discounts) * 100.',
        ),
        source=_m('Κύκλωμα Αγορών (εκπτώσεις γραμμής).', 'Purchases stream (line discounts).'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Πιάνει μόνο εκπτώσεις που γράφτηκαν στη γραμμή του παραστατικού. Πιστώσεις τζίρου και rebates που έρχονται ξεχωριστά δεν φαίνονται εδώ — δες τις Συμφωνίες Προμηθευτών.',
            'Only captures discounts recorded on the document line. Volume rebates issued separately do not appear here — see Supplier Agreements.',
        ),
        circuit='purchases-analytics',
    ),
    Kpi(
        id='purchase_sales_ratio',
        keys=('Ρυθμός αγορών/πωλ.', 'Purchases to sales ratio'),
        title=_m('Ρυθμός Αγορών / Πωλήσεων', 'Purchases / Sales Ratio'),
        what=_m(
            'Δείχνει πόσο βαριές είναι οι αγορές σε σχέση με τις πωλήσεις της ίδιας περιόδου.',
            'Shows how heavy purchasing is relative to sales in the same period.',
        ),
        formula=_m(
            'Ρυθμός = (Αγορές Περιόδου / Πωλήσεις Περιόδου) * 100.',
            'Ratio = (Period Purchases / Period Sales) * 100.',
        ),
        source=_m('Κυκλώματα Πωλήσεων και Αγορών.', 'Sales and Purchases streams.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Πάνω από 100% σημαίνει ότι αγόρασες περισσότερα από όσα πούλησες — χτίζεις απόθεμα. Σε σύντομες περιόδους είναι θορυβώδες γιατί μία μεγάλη παραγγελία το εκτοξεύει.',
            'Above 100% means you bought more than you sold — stock is building. Over short periods it is noisy because one large order spikes it.',
        ),
        circuit='dashboard',
    ),

    # ======================= Απόθεμα / είδη ================================
    Kpi(
        id='stock_value',
        keys=('Αξία Αποθέματος', 'Stock value'),
        title=_m('Αξία Αποθέματος', 'Stock Value'),
        what=_m(
            'Η συνολική αξία αποθέματος των ειδών που πέρασαν τα φίλτρα.',
            'Total stock value for the items passing the filters.',
        ),
        formula=_m(
            'Αξία Αποθέματος = άθροισμα value_amount όλων των ειδών που πέρασαν τα φίλτρα.',
            'Stock Value = sum of value_amount over all filtered items.',
        ),
        source=_m('Snapshot αποθήκης (agg_inventory_snapshot_daily).', 'Warehouse snapshot (agg_inventory_snapshot_daily).'),
        filters=_SNAPSHOT_FILTERS,
        caveats=_m(
            'Είναι φωτογραφία μιας ημερομηνίας, όχι άθροισμα περιόδου. Δεν προστίθεται σε βάθος χρόνου: δύο snapshots δεν αθροίζονται.',
            'It is a point-in-time snapshot, not a period sum. Snapshots do not add up over time.',
        ),
        circuit='inventory-analytics',
    ),
    Kpi(
        id='stock_acquisition_value',
        keys=('Αξία κτήσης αποθήκης', 'Warehouse acquisition value'),
        title=_m('Αξία Κτήσης Αποθήκης', 'Warehouse Acquisition Value'),
        what=_m(
            'Η συνολική αξία κτήσης του αποθέματος στο snapshot που έχεις επιλέξει. Με κλικ ανοίγει ανάλυση χονδρικής, λιανικής και προοπτικού κέρδους.',
            'Total acquisition value of stock at the selected snapshot. Clicking opens a wholesale / retail / prospective-profit breakdown.',
        ),
        formula=_m(
            'Αξία Κτήσης = αξία κόστους αποθέματος μετά τις εκπτώσεις και την αποτίμηση.',
            'Acquisition Value = stock cost value after discounts and valuation.',
        ),
        source=_m('Snapshot αποθήκης.', 'Warehouse snapshot.'),
        filters=_SNAPSHOT_FILTERS,
        caveats=_m(
            'Είναι κόστος, όχι λιανική αξία. Το «προοπτικό κέρδος» στο popup είναι θεωρητικό: υποθέτει ότι όλο το απόθεμα θα πουληθεί στην τρέχουσα λιανική.',
            'This is cost, not retail value. The "prospective profit" in the popup is theoretical: it assumes all stock sells at the current retail price.',
        ),
        circuit='inventory-analytics',
    ),
    Kpi(
        id='qty_on_hand',
        keys=('Ποσότητα σε Απόθεμα', 'Απόθεμα', 'Qty on hand'),
        title=_m('Ποσότητα σε Απόθεμα', 'Quantity On Hand'),
        what=_m('Η συνολική διαθέσιμη ποσότητα όλων των ειδών στο snapshot.', 'Total available quantity of all items in the snapshot.'),
        formula=_m(
            'Ποσότητα σε Απόθεμα = άθροισμα qty_on_hand όλων των ειδών στο snapshot αποθήκης.',
            'Quantity On Hand = sum of qty_on_hand over all items in the warehouse snapshot.',
        ),
        source=_m('Snapshot αποθήκης.', 'Warehouse snapshot.'),
        filters=_SNAPSHOT_FILTERS,
        caveats=_m(
            'Περιλαμβάνει και τη δεσμευμένη ποσότητα. Για πραγματικά διαθέσιμο, αφαίρεσε τη «Δεσμευμένη Ποσότητα».',
            'Includes reserved quantity. For truly available stock, subtract "Reserved Quantity".',
        ),
        circuit='inventory-analytics',
    ),
    Kpi(
        id='qty_reserved',
        keys=('Δεσμευμένη Ποσότητα', 'Reserved quantity'),
        title=_m('Δεσμευμένη Ποσότητα', 'Reserved Quantity'),
        what=_m(
            'Η ποσότητα που είναι δεσμευμένη και δεν είναι άμεσα διαθέσιμη για πώληση.',
            'Quantity reserved and not immediately available for sale.',
        ),
        formula=_m(
            'Δεσμευμένη Ποσότητα = άθροισμα qty_reserved όλων των ειδών στο snapshot αποθήκης.',
            'Reserved Quantity = sum of qty_reserved over all items in the warehouse snapshot.',
        ),
        source=_m('Snapshot αποθήκης.', 'Warehouse snapshot.'),
        filters=_SNAPSHOT_FILTERS,
        caveats=_m(
            'Η δέσμευση προέρχεται από το SoftOne (ανοιχτές παραγγελίες πελατών). Αν δεν χρησιμοποιείτε δεσμεύσεις, θα είναι μόνιμα 0.',
            'Reservations come from SoftOne (open customer orders). If you do not use reservations it stays at 0.',
        ),
        circuit='inventory-analytics',
    ),
    Kpi(
        id='items_total',
        keys=('Σύνολο Ειδών', 'Είδη', 'Total items'),
        title=_m('Σύνολο Ειδών', 'Total Items'),
        what=_m('Το συνολικό πλήθος ειδών που υπάρχουν στο tenant.', 'Total number of items in the tenant.'),
        formula=_m(
            'Σύνολο Ειδών = count(όλα τα item codes που πέρασαν τα ενεργά φίλτρα).',
            'Total Items = count(all item codes passing the active filters).',
        ),
        source=_m('Αρχείο ειδών (item_master, από SoftOne).', 'Item master (item_master, from SoftOne).'),
        filters=_m(
            'Κατηγορία, brand, ομάδα, προμηθευτής, status και αναζήτηση.',
            'Category, brand, group, supplier, status and search.',
        ),
        caveats=_m(
            'Μετράει κωδικούς ειδών, όχι τεμάχια. Περιλαμβάνει και είδη χωρίς κίνηση και χωρίς απόθεμα.',
            'Counts item codes, not units. Includes items with no movement and no stock.',
        ),
        circuit='warehouse-items',
    ),
    Kpi(
        id='items_active',
        keys=('Ενεργά Είδη', 'Active items'),
        title=_m('Ενεργά Είδη', 'Active Items'),
        what=_m(
            'Πόσα είδη είναι ενεργά στο SoftOne (πρωτογενής πηγή, πεδίο ISACTIVE).',
            'How many items are active in SoftOne (primary source, ISACTIVE field).',
        ),
        formula=_m(
            'Ενεργά Είδη = count(items) όπου ISACTIVE = true στο SoftOne.',
            'Active Items = count(items) where ISACTIVE = true in SoftOne.',
        ),
        source=_m('item_master — το SoftOne είναι αυθεντία για το status.', 'item_master — SoftOne is authoritative for status.'),
        filters=_m('Τα φίλτρα ειδών της σελίδας.', 'The page item filters.'),
        caveats=_m(
            '«Ενεργό» είναι διαχειριστικό flag του SoftOne, όχι ένδειξη ότι το είδος κινείται. Για πραγματική κίνηση δες το «Πουλήθηκαν σε περίοδο».',
            '"Active" is a SoftOne administrative flag, not proof the item moves. For real movement see "Sold in period".',
        ),
        circuit='warehouse-items',
    ),
    Kpi(
        id='items_inactive',
        keys=('Ανενεργά Είδη', 'Inactive items'),
        title=_m('Ανενεργά Είδη', 'Inactive Items'),
        what=_m(
            'Πόσα είδη είναι ανενεργά στο SoftOne (πρωτογενής πηγή, πεδίο ISACTIVE).',
            'How many items are inactive in SoftOne (primary source, ISACTIVE field).',
        ),
        formula=_m(
            'Ανενεργά Είδη = count(items) όπου ISACTIVE = false στο SoftOne.',
            'Inactive Items = count(items) where ISACTIVE = false in SoftOne.',
        ),
        source=_m('item_master.', 'item_master.'),
        filters=_m('Τα φίλτρα ειδών της σελίδας.', 'The page item filters.'),
        caveats=_m(
            'Ανενεργό είδος μπορεί να έχει ακόμη απόθεμα και ιστορικές πωλήσεις.',
            'An inactive item may still carry stock and historical sales.',
        ),
        circuit='warehouse-items',
    ),
    Kpi(
        id='items_sold_in_period',
        keys=('Πουλήθηκαν σε περίοδο', 'Sold in period'),
        title=_m('Πουλήθηκαν σε Περίοδο', 'Sold in Period'),
        what=_m(
            'Πόσα είδη είχαν τουλάχιστον μία πώληση μέσα στην περίοδο κίνησης — καθαρός δείκτης κίνησης, ανεξάρτητος από ενεργό/ανενεργό.',
            'How many items had at least one sale within the movement window — a pure movement signal, independent of active/inactive.',
        ),
        formula=_m(
            'Πουλήθηκαν σε περίοδο = count(items) με ≥1 πώληση στις τελευταίες X ημέρες (movement window).',
            'Sold in period = count(items) with ≥1 sale in the last X days (movement window).',
        ),
        source=_m('Κύκλωμα Πωλήσεων διασταυρωμένο με το αρχείο ειδών.', 'Sales stream cross-referenced with the item master.'),
        filters=_m(
            'Το παράθυρο κίνησης ορίζεται στις ρυθμίσεις του tenant (τυπικά 120 ημέρες).',
            'The movement window is set in tenant settings (typically 120 days).',
        ),
        caveats=_m(
            'Μετράει κωδικούς με κίνηση, όχι τεμάχια ή αξία. Ένα είδος με μία μοναδική πώληση μετράει το ίδιο με ένα best seller.',
            'Counts moving item codes, not units or value. An item with a single sale counts the same as a best seller.',
        ),
        circuit='warehouse-items',
    ),
    Kpi(
        id='items_fast',
        keys=('Ταχυκίνητα Είδη', 'Fast-moving items'),
        title=_m('Ταχυκίνητα Είδη', 'Fast-Moving Items'),
        what=_m(
            'Πόσα είδη έχουν χαρακτηριστεί ως ταχυκίνητα με βάση το ruleset του tenant.',
            'How many items are classified as fast-moving under the tenant ruleset.',
        ),
        formula=_m(
            "Ταχυκίνητα Είδη = count(items) όπου status κίνησης = 'ταχυκίνητο'.",
            "Fast-Moving Items = count(items) where movement status = 'fast'.",
        ),
        source=_m('Κανόνες κατάταξης του tenant πάνω στο κύκλωμα πωλήσεων.', 'Tenant classification rules over the sales stream.'),
        filters=_m('Τα φίλτρα ειδών και η περίοδος κίνησης.', 'Item filters and the movement window.'),
        caveats=_m(
            'Το όριο «ταχυκίνητο» είναι παραμετρικό ανά tenant. Δύο tenants με τα ίδια δεδομένα μπορεί να δίνουν διαφορετικά νούμερα.',
            'The fast-moving threshold is per-tenant configurable. Two tenants with identical data can report different counts.',
        ),
        circuit='warehouse-items',
    ),
    Kpi(
        id='items_slow',
        keys=('Αργοκίνητα Είδη', 'Slow-moving items'),
        title=_m('Αργοκίνητα Είδη', 'Slow-Moving Items'),
        what=_m(
            'Πόσα είδη έχουν χαρακτηριστεί ως αργοκίνητα με βάση το ruleset του tenant.',
            'How many items are classified as slow-moving under the tenant ruleset.',
        ),
        formula=_m(
            "Αργοκίνητα Είδη = count(items) όπου status κίνησης = 'αργοκίνητο'.",
            "Slow-Moving Items = count(items) where movement status = 'slow'.",
        ),
        source=_m('Κανόνες κατάταξης του tenant.', 'Tenant classification rules.'),
        filters=_m('Τα φίλτρα ειδών και η περίοδος κίνησης.', 'Item filters and the movement window.'),
        caveats=_m(
            'Αργοκίνητο δεν σημαίνει αυτόματα προς διαγραφή: έλεγξε πρώτα δεσμευμένο κεφάλαιο και εποχικότητα στο Destocking.',
            'Slow-moving does not automatically mean delist: check tied-up capital and seasonality in Destocking first.',
        ),
        circuit='warehouse-items',
    ),
    Kpi(
        id='items_abc',
        keys=('Χωρίς ABC', 'ABC', 'ABC κατηγορία'),
        title=_m('Κατηγορία ABC', 'ABC Category'),
        what=_m(
            'Πόσα είδη ανήκουν στη συγκεκριμένη ABC / παραγγελτική κατηγορία.',
            'How many items belong to the given ABC / ordering category.',
        ),
        formula=_m(
            'ABC = manual_order_category από SoftOne, με fallback στο abc_category.',
            'ABC = manual_order_category from SoftOne, falling back to abc_category.',
        ),
        source=_m('item_master — έρχεται από το SoftOne, δεν υπολογίζεται από το BI.', 'item_master — comes from SoftOne, not computed by BI.'),
        filters=_m('Τα φίλτρα ειδών της σελίδας.', 'The page item filters.'),
        caveats=_m(
            'Η κατάταξη ABC δεν υπολογίζεται από το BI. «Χωρίς ABC» σημαίνει ότι το είδος δεν έχει κατηγορία στο SoftOne.',
            'ABC is not computed by BI. "No ABC" means the item carries no category in SoftOne.',
        ),
        circuit='warehouse-items',
    ),
    Kpi(
        id='items_commercial_status',
        keys=('Χωρίς Εμπορικό Status', 'Εμπορικό Status', 'Commercial status'),
        title=_m('Εμπορικό Status', 'Commercial Status'),
        what=_m('Πόσα είδη ανήκουν στο συγκεκριμένο εμπορικό status.', 'How many items carry the given commercial status.'),
        formula=_m('Εμπορικό Status = UTBL05 από το SoftOne item extra.', 'Commercial Status = UTBL05 from the SoftOne item extra fields.'),
        source=_m('item_master (SoftOne UTBL05).', 'item_master (SoftOne UTBL05).'),
        filters=_m('Τα φίλτρα ειδών της σελίδας.', 'The page item filters.'),
        caveats=_m(
            'Ορίζεται αποκλειστικά στο SoftOne. Αν λείπει, διορθώνεται εκεί και έρχεται με τον επόμενο συγχρονισμό.',
            'Set exclusively in SoftOne. If missing, fix it there and it arrives with the next sync.',
        ),
        circuit='warehouse-items',
    ),

    # ======================= Οικονομικά ====================================
    Kpi(
        id='receivables_total',
        keys=('Συνολικές Απαιτήσεις', 'Απαιτήσεις', 'Total receivables'),
        title=_m('Συνολικές Απαιτήσεις', 'Total Receivables'),
        what=_m('Το σύνολο των ανοικτών υπολοίπων πελατών.', 'Total open customer balances.'),
        formula=_m(
            'Συνολικές Απαιτήσεις = άθροισμα open_balance όλων των πελατών που πέρασαν τα φίλτρα.',
            'Total Receivables = sum of open_balance over all filtered customers.',
        ),
        source=_m('Κύκλωμα Υπολοίπων Πελατών.', 'Customer balances stream.'),
        filters=_m('Υποκατάστημα, πελάτης και αναζήτηση.', 'Branch, customer and search.'),
        caveats=_m(
            'Είναι τρέχον υπόλοιπο, όχι υπόλοιπο περιόδου: δεν επηρεάζεται από το «Από/Έως». Περιλαμβάνει και μη ληξιπρόθεσμα.',
            'This is the current balance, not a period balance: From/To does not affect it. It includes amounts not yet due.',
        ),
        circuit='customer-balances',
    ),
    Kpi(
        id='receivables_overdue',
        keys=('Ληξιπρόθεσμες', 'Ληξιπρόθεσμες Απαιτήσεις', 'Overdue receivables'),
        title=_m('Ληξιπρόθεσμες Απαιτήσεις', 'Overdue Receivables'),
        what=_m('Το μέρος των απαιτήσεων που έχει ήδη λήξει.', 'The share of receivables already past due.'),
        formula=_m(
            'Ληξιπρόθεσμες = άθροισμα open_balance όπου due_date < σήμερα. Ποσοστό = (Ληξιπρόθεσμες / Συνολικές Απαιτήσεις) * 100.',
            'Overdue = sum of open_balance where due_date < today. Share = (Overdue / Total Receivables) * 100.',
        ),
        source=_m('Κύκλωμα Υπολοίπων Πελατών.', 'Customer balances stream.'),
        filters=_m('Υποκατάστημα και πελάτης.', 'Branch and customer.'),
        caveats=_m(
            'Χρειάζεται ημερομηνία λήξης στο παραστατικό. Υπόλοιπα χωρίς due_date δεν μπορούν να χαρακτηριστούν ληξιπρόθεσμα και μένουν εκτός.',
            'Requires a due date on the document. Balances without due_date cannot be marked overdue and stay out.',
        ),
        circuit='customer-balances',
    ),
    Kpi(
        id='payables_total',
        keys=('Υποχρ. Προμηθευτών', 'Υποχρεώσεις Προμηθευτών', 'Υποχρεώσεις', 'Supplier payables'),
        title=_m('Υποχρεώσεις Προμηθευτών', 'Supplier Payables'),
        what=_m('Το σύνολο των ανοικτών υποχρεώσεων προς προμηθευτές.', 'Total open payables to suppliers.'),
        formula=_m(
            'Υποχρεώσεις = άθροισμα open_balance όλων των προμηθευτών. Ληξιπρόθεσμα = άθροισμα open_balance όπου due_date < σήμερα.',
            'Payables = sum of open_balance over all suppliers. Overdue = sum of open_balance where due_date < today.',
        ),
        source=_m('Κύκλωμα Υπολοίπων Προμηθευτών.', 'Supplier balances stream.'),
        filters=_m('Προμηθευτής και αναζήτηση.', 'Supplier and search.'),
        caveats=_m(
            'Τρέχον υπόλοιπο, ανεξάρτητο από την περίοδο. Πιστωτικά υπόλοιπα προμηθευτή μειώνουν το σύνολο.',
            'Current balance, independent of the period. Supplier credit balances reduce the total.',
        ),
        circuit='supplier-balances',
    ),
    Kpi(
        id='cash_in',
        keys=('Εισροές', 'Cash in'),
        title=_m('Εισροές', 'Cash In'),
        what=_m(
            'Το σύνολο των εισροών στο ταμείο και στους λογαριασμούς για το επιλεγμένο διάστημα.',
            'Total inflows into cash and accounts for the selected period.',
        ),
        formula=_m(
            'Εισροές = άθροισμα amount των εγγραφών cashflow με θετικό πρόσημο ή τύπο inflow στο [Από, Έως].',
            'Cash In = sum of cashflow amounts with a positive sign or inflow type within [From, To].',
        ),
        source=_m('Κύκλωμα Χρηματοροών (cash transactions).', 'Cash transactions stream.'),
        filters=_m('Περίοδος, λογαριασμός, υποκατάστημα και κατηγορία κίνησης.', 'Period, account, branch and transaction category.'),
        caveats=_m(
            'Είναι ταμειακή κίνηση, όχι τζίρος. Μια πώληση επί πιστώσει αυξάνει τον τζίρο σήμερα και τις εισροές όταν εισπραχθεί.',
            'This is cash movement, not turnover. A credit sale raises turnover today and cash in only when collected.',
        ),
        circuit='cash-transactions',
    ),
    Kpi(
        id='cash_out',
        keys=('Εκροές', 'Cash out'),
        title=_m('Εκροές', 'Cash Out'),
        what=_m(
            'Το σύνολο των εκροών από ταμείο και λογαριασμούς για το επιλεγμένο διάστημα.',
            'Total outflows from cash and accounts for the selected period.',
        ),
        formula=_m(
            'Εκροές = άθροισμα amount των εγγραφών cashflow με αρνητικό πρόσημο ή τύπο outflow στο [Από, Έως].',
            'Cash Out = sum of cashflow amounts with a negative sign or outflow type within [From, To].',
        ),
        source=_m('Κύκλωμα Χρηματοροών.', 'Cash transactions stream.'),
        filters=_m('Περίοδος, λογαριασμός, υποκατάστημα και κατηγορία κίνησης.', 'Period, account, branch and transaction category.'),
        caveats=_m(
            'Περιλαμβάνει πληρωμές προμηθευτών, έξοδα και μεταφορές μεταξύ λογαριασμών. Οι μεταφορές δεν είναι πραγματική δαπάνη — φίλτραρε ανά κατηγορία.',
            'Includes supplier payments, expenses and transfers between accounts. Transfers are not real spend — filter by category.',
        ),
        circuit='cash-transactions',
    ),
    Kpi(
        id='net_cash',
        keys=('Καθαρή Ροή', 'Ταμειακή Ροή', 'Καθαρή Ταμειακή Ροή', 'Net cash flow'),
        title=_m('Καθαρή Ταμειακή Ροή', 'Net Cash Flow'),
        what=_m('Το καθαρό αποτέλεσμα ρευστότητας στο επιλεγμένο διάστημα.', 'Net liquidity result for the selected period.'),
        formula=_m('Καθαρή Ροή = Εισροές - Εκροές.', 'Net Cash Flow = Cash In - Cash Out.'),
        source=_m('Κύκλωμα Χρηματοροών.', 'Cash transactions stream.'),
        filters=_m('Περίοδος, λογαριασμός, υποκατάστημα και κατηγορία.', 'Period, account, branch and category.'),
        caveats=_m(
            'Αρνητική ροή σε έναν μήνα δεν σημαίνει ζημιά — μπορεί να είναι πληρωμή προμηθευτών ή χτίσιμο αποθέματος. Διάβασέ το μαζί με το μικτό κέρδος.',
            'Negative flow in a month does not mean a loss — it can be supplier payments or stock build. Read it alongside gross profit.',
        ),
        circuit='cash-transactions',
    ),
    Kpi(
        id='cash_entries',
        keys=('Εγγραφές', 'Πλήθος Εγγραφών', 'Entries'),
        title=_m('Πλήθος Εγγραφών', 'Number of Entries'),
        what=_m('Πόσες κινήσεις βρέθηκαν στο επιλεγμένο διάστημα.', 'How many transactions were found in the selected period.'),
        formula=_m('Πλήθος Εγγραφών = count(εγγραφές) στο [Από, Έως] μετά τα φίλτρα.', 'Entries = count(records) within [From, To] after filters.'),
        source=_m('Το κύκλωμα της τρέχουσας σελίδας (χρηματοροές ή έξοδα).', 'The current page stream (cash flow or expenses).'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Μετράει γραμμές κινήσεων, όχι παραστατικά. Ένα παραστατικό με πολλές γραμμές μετράει πολλές φορές.',
            'Counts transaction rows, not documents. A multi-line document counts multiple times.',
        ),
        circuit='cash-transactions',
    ),

    # ======================= Έξοδα =========================================
    Kpi(
        id='expenses_total',
        keys=('Σύνολο Εξόδων', 'Total expenses'),
        title=_m('Σύνολο Εξόδων', 'Total Expenses'),
        what=_m('Το σύνολο των λειτουργικών εξόδων στο επιλεγμένο διάστημα.', 'Total operating expenses in the selected period.'),
        formula=_m(
            'Σύνολο Εξόδων = άθροισμα amount όλων των εγγραφών εξόδων στο [Από, Έως].',
            'Total Expenses = sum of amount over expense records within [From, To].',
        ),
        source=_m('Κύκλωμα Λειτουργικών Εξόδων.', 'Operating expenses stream.'),
        filters=_m('Περίοδος, κατηγορία εξόδου, υποκατάστημα και προμηθευτής.', 'Period, expense category, branch and supplier.'),
        caveats=_m(
            'Είναι λειτουργικά έξοδα, ΟΧΙ αγορές εμπορευμάτων. Οι αγορές μετριούνται στο κύκλωμα Αγορών — μη τα αθροίσεις χωρίς έλεγχο διπλοεγγραφής.',
            'These are operating expenses, NOT merchandise purchases. Purchases live in the Purchases stream — do not add the two without checking for double counting.',
        ),
        circuit='operating-expenses',
    ),
    Kpi(
        id='expense_ratio',
        keys=('Έξοδα / Έσοδα', 'Expense ratio', 'Expenses to revenue'),
        title=_m('Έξοδα / Έσοδα', 'Expenses / Revenue'),
        what=_m(
            'Το ποσοστό που αντιστοιχούν τα έξοδα πάνω στα έσοδα της ίδιας περιόδου.',
            'Expenses as a percentage of revenue for the same period.',
        ),
        formula=_m(
            'Έξοδα / Έσοδα = (Σύνολο Εξόδων / Πωλήσεις Περιόδου) * 100.',
            'Expenses / Revenue = (Total Expenses / Period Sales) * 100.',
        ),
        source=_m('Κυκλώματα Λειτουργικών Εξόδων και Πωλήσεων.', 'Operating expenses and Sales streams.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Συγκρίνει έξοδα με τζίρο, όχι με μικτό κέρδος. Για να δεις αν μένει αποτέλεσμα, σύγκρινε τα έξοδα με το μικτό κέρδος της ίδιας περιόδου.',
            'Compares expenses to turnover, not to gross profit. To see what is left, compare expenses against the same period\'s gross profit.',
        ),
        circuit='operating-expenses',
    ),
    Kpi(
        id='expense_categories',
        keys=('Κατηγορίες', 'Categories'),
        title=_m('Κατηγορίες Εξόδων', 'Expense Categories'),
        what=_m('Πόσες διαφορετικές κατηγορίες εξόδων συμμετείχαν στο διάστημα.', 'How many distinct expense categories occurred in the period.'),
        formula=_m(
            'Κατηγορίες = count(distinct category) στις εγγραφές εξόδων του [Από, Έως].',
            'Categories = count(distinct category) over expense records within [From, To].',
        ),
        source=_m('Κύκλωμα Λειτουργικών Εξόδων.', 'Operating expenses stream.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Πολλές κατηγορίες με μικρά ποσά συνήθως σημαίνει ασυνεπή κωδικοποίηση στο SoftOne.',
            'Many categories with tiny amounts usually means inconsistent coding in SoftOne.',
        ),
        circuit='operating-expenses',
    ),

    # ======================= Υποκαταστήματα ================================
    Kpi(
        id='active_branches',
        keys=('Ενεργά υποκαταστήματα', 'Active branches'),
        title=_m('Ενεργά Υποκαταστήματα', 'Active Branches'),
        what=_m('Πόσα υποκαταστήματα έχουν θετικό τζίρο στο έτος μέχρι σήμερα.', 'How many branches show positive turnover year to date.'),
        formula=_m(
            'Ενεργά Υποκαταστήματα = count(υποκατάστημα) όπου Τζίρος Έτους YTD > 0.',
            'Active Branches = count(branch) where YTD turnover > 0.',
        ),
        source=_m('Κύκλωμα Πωλήσεων ανά υποκατάστημα.', 'Sales stream by branch.'),
        filters=_m('Το cutoff έρχεται από την ημερομηνία «Έως».', 'The cutoff comes from the "To" date.'),
        caveats=_m(
            'Μετράει υποκαταστήματα με κίνηση, όχι όσα υπάρχουν στο αρχείο. Νέο υποκατάστημα χωρίς πωλήσεις δεν μετράει.',
            'Counts trading branches, not registered ones. A new branch with no sales is not counted.',
        ),
        circuit='dashboard',
    ),
    Kpi(
        id='top_branch_share',
        keys=('Κορυφαίο κατάστημα', 'Συγκέντρωση Κορυφαίου Καταστήματος', 'Top branch concentration'),
        title=_m('Συγκέντρωση Κορυφαίου Καταστήματος', 'Top Branch Concentration'),
        what=_m(
            'Το ποσοστό που συμμετέχει το πιο δυνατό υποκατάστημα στον συνολικό ετήσιο τζίρο.',
            'The share of total annual turnover produced by the strongest branch.',
        ),
        formula=_m(
            'Συγκέντρωση = (μεγαλύτερος τζίρος υποκαταστήματος YTD / συνολικός τζίρος YTD) * 100.',
            'Concentration = (max branch YTD turnover / total YTD turnover) * 100.',
        ),
        source=_m('Κύκλωμα Πωλήσεων ανά υποκατάστημα.', 'Sales stream by branch.'),
        filters=_m('Το cutoff έρχεται από την ημερομηνία «Έως».', 'The cutoff comes from the "To" date.'),
        caveats=_m(
            'Δείκτης συγκέντρωσης κινδύνου: υψηλό ποσοστό σημαίνει εξάρτηση από ένα σημείο. Σε μονοκατάστημα είναι πάντα 100%.',
            'A risk-concentration gauge: a high share means dependence on one site. For a single-site business it is always 100%.',
        ),
        circuit='dashboard',
    ),

    # ======================= POS ===========================================
    Kpi(
        id='pos_receipts',
        keys=('Αποδείξεις Περιόδου', 'Receipts'),
        title=_m('Αποδείξεις Περιόδου', 'Receipts in Period'),
        what=_m('Πόσες αποδείξεις λιανικής εκδόθηκαν στο επιλεγμένο διάστημα.', 'How many retail receipts were issued in the selected period.'),
        formula=_m(
            'Αποδείξεις Περιόδου = count(παραστατικά λιανικής) στο [Από, Έως].',
            'Receipts = count(retail documents) within [From, To].',
        ),
        source=_m('Κύκλωμα Πωλήσεων, παραστατικά λιανικής.', 'Sales stream, retail documents.'),
        filters=_m('Περίοδος, υποκατάστημα, ταμείο και σειρά.', 'Period, branch, register and series.'),
        caveats=_m(
            'Μετράει παραστατικά, όχι πελάτες: ο ίδιος πελάτης σε δύο επισκέψεις μετράει δύο φορές. Οι ακυρώσεις εξαρτώνται από τη ρύθμιση συμπεριφορών παραστατικών.',
            'Counts documents, not customers: the same customer visiting twice counts twice. Voids depend on the document behaviour configuration.',
        ),
        circuit='pos',
    ),
    Kpi(
        id='pos_total_collected',
        keys=('Σύνολο Εισπράξεων', 'Total collected'),
        title=_m('Σύνολο Εισπράξεων', 'Total Collected'),
        what=_m('Η συνολική αξία που εισπράχθηκε από λιανική στο διάστημα.', 'Total retail value collected in the period.'),
        formula=_m(
            'Σύνολο Εισπράξεων = άθροισμα αξίας αποδείξεων λιανικής στο [Από, Έως].',
            'Total Collected = sum of retail receipt value within [From, To].',
        ),
        source=_m('Κύκλωμα Πωλήσεων, παραστατικά λιανικής.', 'Sales stream, retail documents.'),
        filters=_m('Περίοδος, υποκατάστημα, ταμείο, τρόπος πληρωμής.', 'Period, branch, register, payment method.'),
        caveats=_m(
            'Δεν είναι το ίδιο με τις «Εισροές» των χρηματοροών: εδώ μετράνε παραστατικά λιανικής, εκεί ταμειακές κινήσεις κάθε είδους.',
            'Not the same as cash-flow "Cash In": here retail documents are counted, there any kind of cash movement is.',
        ),
        circuit='pos',
    ),
    Kpi(
        id='pos_avg_receipt',
        keys=('Μέση Αξία Απόδειξης', 'Average receipt value', 'Basket value'),
        title=_m('Μέση Αξία Απόδειξης', 'Average Receipt Value'),
        what=_m('Πόσο ξοδεύει κατά μέσο όρο ένας πελάτης ανά επίσκεψη.', 'Average spend per customer visit.'),
        formula=_m(
            'Μέση Αξία Απόδειξης = Σύνολο Εισπράξεων / Αποδείξεις Περιόδου.',
            'Average Receipt Value = Total Collected / Receipts in Period.',
        ),
        source=_m('Κύκλωμα Πωλήσεων, παραστατικά λιανικής.', 'Sales stream, retail documents.'),
        filters=_m('Περίοδος, υποκατάστημα, ταμείο και σειρά.', 'Period, branch, register and series.'),
        caveats=_m(
            'Είναι ο βασικός δείκτης «καλαθιού». Πέφτει όταν αυξάνονται οι μικρές επισκέψεις, ακόμη κι αν ο τζίρος ανεβαίνει.',
            'This is the core "basket" metric. It drops when small visits increase, even while turnover grows.',
        ),
        circuit='pos',
    ),
    Kpi(
        id='pos_avg_items',
        keys=('Μέσος Αριθμός Ειδών', 'Average items per receipt'),
        title=_m('Μέσος Αριθμός Ειδών', 'Average Items per Receipt'),
        what=_m('Πόσα διαφορετικά είδη περιέχει κατά μέσο όρο μια απόδειξη.', 'How many items an average receipt contains.'),
        formula=_m(
            'Μέσος Αριθμός Ειδών = σύνολο γραμμών αποδείξεων / πλήθος αποδείξεων.',
            'Average Items = total receipt lines / number of receipts.',
        ),
        source=_m('Κύκλωμα Πωλήσεων, γραμμές λιανικής.', 'Sales stream, retail lines.'),
        filters=_m('Περίοδος, υποκατάστημα, ταμείο και σειρά.', 'Period, branch, register and series.'),
        caveats=_m(
            'Δείκτης cross-selling. Άνοδος εδώ με σταθερή μέση αξία απόδειξης σημαίνει ότι πουλάς περισσότερα φθηνά είδη.',
            'A cross-selling gauge. Rising here with a flat average receipt value means you are selling more cheap items.',
        ),
        circuit='pos',
    ),

    # ======================= E-shop ========================================
    Kpi(
        id='eshop_revenue',
        keys=('Έσοδα E-Shop', 'E-shop revenue'),
        title=_m('Έσοδα E-Shop', 'E-Shop Revenue'),
        what=_m('Ο τζίρος που προήλθε από το ηλεκτρονικό κατάστημα στο διάστημα.', 'Turnover originating from the online store in the period.'),
        formula=_m(
            'Έσοδα E-Shop = άθροισμα καθαρής αξίας παραστατικών με κανάλι e-shop στο [Από, Έως].',
            'E-Shop Revenue = sum of net value of documents in the e-shop channel within [From, To].',
        ),
        source=_m('Κύκλωμα Πωλήσεων, φιλτραρισμένο στο κανάλι e-shop.', 'Sales stream filtered to the e-shop channel.'),
        filters=_m('Περίοδος, κανάλι, τρόπος εκτέλεσης και μεταφορική.', 'Period, channel, fulfilment model and carrier.'),
        caveats=_m(
            'Η αναγνώριση καναλιού βασίζεται στη σειρά/τύπο παραστατικού που έχει οριστεί ως e-shop. Αν οι σειρές δεν είναι σωστά ρυθμισμένες, ο διαχωρισμός θα είναι λάθος.',
            'Channel detection relies on the document series/type configured as e-shop. If series are misconfigured, the split will be wrong.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_orders',
        keys=('Παραγγελίες Site', 'Site orders'),
        title=_m('Παραγγελίες Site', 'Site Orders'),
        what=_m('Πόσες παραγγελίες καταχωρήθηκαν από το e-shop στο διάστημα.', 'How many orders were placed through the e-shop in the period.'),
        formula=_m('Παραγγελίες Site = count(παραστατικά καναλιού e-shop) στο [Από, Έως].', 'Site Orders = count(e-shop channel documents) within [From, To].'),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος, μοντέλο εκτέλεσης, μεταφορική και πόλη παράδοσης.', 'Period, fulfilment model, carrier and delivery city.'),
        caveats=_m(
            'Μετράει παραστατικά, όχι μοναδικούς πελάτες.',
            'Counts documents, not unique customers.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_avg_order',
        keys=('Μέση Παραγγελία', 'Average order value'),
        title=_m('Μέση Παραγγελία', 'Average Order Value'),
        what=_m('Η μέση αξία μιας παραγγελίας e-shop.', 'The average value of an e-shop order.'),
        formula=_m('Μέση Παραγγελία = Έσοδα E-Shop / Παραγγελίες Site.', 'Average Order Value = E-Shop Revenue / Site Orders.'),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος και φίλτρα e-shop.', 'Period and e-shop filters.'),
        caveats=_m(
            'Σύγκρινέ την με το κόστος αποστολής ανά παραγγελία: αν η μέση παραγγελία πέφτει κοντά στο κόστος courier, το κανάλι χάνει.',
            'Compare against shipping cost per order: when the average order approaches courier cost, the channel loses money.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_shipping_cost',
        keys=('Έξοδα Αποστολής', 'Shipping cost'),
        title=_m('Έξοδα Αποστολής', 'Shipping Cost'),
        what=_m('Το συνολικό κόστος αποστολών για τις παραγγελίες e-shop του διαστήματος.', 'Total shipping cost for e-shop orders in the period.'),
        formula=_m(
            'Έξοδα Αποστολής = άθροισμα χρεώσεων μεταφορικών στα παραστατικά e-shop στο [Από, Έως].',
            'Shipping Cost = sum of carrier charges on e-shop documents within [From, To].',
        ),
        source=_m('Έξοδα παραστατικών του καναλιού e-shop.', 'Document charges on e-shop channel documents.'),
        filters=_m('Περίοδος, μεταφορική και μοντέλο εκτέλεσης.', 'Period, carrier and fulfilment model.'),
        caveats=_m(
            'Πιάνει μόνο ό,τι έχει καταχωρηθεί ως έξοδο στο παραστατικό. Τιμολόγια courier που έρχονται συγκεντρωτικά δεν φαίνονται εδώ.',
            'Only captures charges recorded on the document. Consolidated courier invoices do not appear here.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_cod_cost',
        keys=('Έξοδα Αντικαταβολής', 'Cash on delivery cost'),
        title=_m('Έξοδα Αντικαταβολής', 'Cash-on-Delivery Cost'),
        what=_m('Το κόστος των αντικαταβολών για τις παραγγελίες του διαστήματος.', 'Cost of cash-on-delivery for the period\'s orders.'),
        formula=_m(
            'Έξοδα Αντικαταβολής = άθροισμα χρεώσεων αντικαταβολής στα παραστατικά e-shop στο [Από, Έως].',
            'COD Cost = sum of cash-on-delivery charges on e-shop documents within [From, To].',
        ),
        source=_m('Έξοδα παραστατικών του καναλιού e-shop.', 'Document charges on e-shop channel documents.'),
        filters=_m('Περίοδος, τρόπος πληρωμής και μεταφορική.', 'Period, payment method and carrier.'),
        caveats=_m(
            'Αφορά μόνο παραγγελίες με αντικαταβολή. Αν αυξάνεται δυσανάλογα, εξέτασε κίνητρα για προπληρωμή.',
            'Applies only to COD orders. If it grows disproportionately, consider prepayment incentives.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_other_charges',
        keys=('Λοιπές Επιβαρύνσεις', 'Other charges'),
        title=_m('Λοιπές Επιβαρύνσεις', 'Other Charges'),
        what=_m('Λοιπά έξοδα παραστατικού που βαραίνουν τις παραγγελίες e-shop.', 'Other document charges loading e-shop orders.'),
        formula=_m(
            'Λοιπές Επιβαρύνσεις = άθροισμα λοιπών εξόδων παραστατικού, εκτός αποστολής και αντικαταβολής.',
            'Other Charges = sum of document charges other than shipping and COD.',
        ),
        source=_m('Έξοδα παραστατικών του καναλιού e-shop.', 'Document charges on e-shop channel documents.'),
        filters=_m('Περίοδος και φίλτρα e-shop.', 'Period and e-shop filters.'),
        caveats=_m(
            'Μεγάλο ποσό εδώ συνήθως σημαίνει ότι κάποιο έξοδο δεν έχει δική του κατηγορία στο SoftOne.',
            'A large amount here usually means some charge lacks its own category in SoftOne.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_courier_ratio',
        keys=('Κόστος courier / έσοδα', 'Courier cost to revenue'),
        title=_m('Κόστος Courier / Έσοδα', 'Courier Cost / Revenue'),
        what=_m('Πόσο του τζίρου e-shop τρώνε τα μεταφορικά.', 'How much of e-shop turnover shipping consumes.'),
        formula=_m(
            'Κόστος Courier / Έσοδα = ((Έξοδα Αποστολής + Έξοδα Αντικαταβολής) / Έσοδα E-Shop) * 100.',
            'Courier Cost / Revenue = ((Shipping Cost + COD Cost) / E-Shop Revenue) * 100.',
        ),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος, μεταφορική και μοντέλο εκτέλεσης.', 'Period, carrier and fulfilment model.'),
        caveats=_m(
            'Σύγκρινέ το με το μικτό περιθώριο του καναλιού: αν το ποσοστό πλησιάζει το περιθώριο, οι παραγγελίες δεν αφήνουν κέρδος.',
            'Compare it to the channel gross margin: when the ratio approaches the margin, orders leave no profit.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_shipments',
        keys=('Αποστολές', 'Shipments'),
        title=_m('Αποστολές', 'Shipments'),
        what=_m('Πόσες αποστολές έγιναν για τις παραγγελίες του διαστήματος.', 'How many shipments were made for the period\'s orders.'),
        formula=_m('Αποστολές = count(παραστατικά e-shop με μεταφορική) στο [Από, Έως].', 'Shipments = count(e-shop documents with a carrier) within [From, To].'),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος, μεταφορική και πόλη παράδοσης.', 'Period, carrier and delivery city.'),
        caveats=_m(
            'Διαφέρει από τις «Παραγγελίες Site» όταν υπάρχουν παραλαβές από κατάστημα ή παραγγελίες χωρίς μεταφορική.',
            'Differs from "Site Orders" when there are click-and-collect pickups or orders without a carrier.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_orders_no_carrier',
        keys=('Παραγγελίες χωρίς μεταφορική', 'Orders without carrier'),
        title=_m('Παραγγελίες Χωρίς Μεταφορική', 'Orders Without a Carrier'),
        what=_m(
            'Παραγγελίες e-shop που δεν έχουν καταχωρημένη μεταφορική — συνήθως παραλαβή από κατάστημα ή ελλιπής καταχώρηση.',
            'E-shop orders with no carrier recorded — usually click-and-collect or incomplete data entry.',
        ),
        formula=_m(
            'Παραγγελίες χωρίς μεταφορική = count(παραστατικά e-shop όπου μεταφορική είναι κενή).',
            'Orders Without a Carrier = count(e-shop documents where carrier is empty).',
        ),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος και φίλτρα e-shop.', 'Period and e-shop filters.'),
        caveats=_m(
            'Δείκτης ποιότητας δεδομένων: μεγάλο νούμερο σημαίνει ότι το κόστος αποστολής υποεκτιμάται.',
            'A data-quality signal: a large number means shipping cost is understated.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_top_carrier',
        keys=('Κύρια μεταφορική', 'Top carrier'),
        title=_m('Κύρια Μεταφορική', 'Top Carrier'),
        what=_m('Η μεταφορική με τις περισσότερες αποστολές στο διάστημα.', 'The carrier with the most shipments in the period.'),
        formula=_m('Κύρια Μεταφορική = η μεταφορική με το μεγαλύτερο count αποστολών.', 'Top Carrier = the carrier with the highest shipment count.'),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος και φίλτρα e-shop.', 'Period and e-shop filters.'),
        caveats=_m(
            'Επιλέγεται με βάση το πλήθος αποστολών, όχι το κόστος. Η ακριβότερη μπορεί να είναι άλλη.',
            'Chosen by shipment count, not cost. The most expensive one may be different.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_top_city',
        keys=('Top πόλη παράδοσης', 'Top delivery city'),
        title=_m('Top Πόλη Παράδοσης', 'Top Delivery City'),
        what=_m('Η πόλη με τις περισσότερες παραδόσεις στο διάστημα.', 'The city with the most deliveries in the period.'),
        formula=_m('Top Πόλη = η πόλη παράδοσης με το μεγαλύτερο count παραγγελιών.', 'Top City = the delivery city with the highest order count.'),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος και φίλτρα e-shop.', 'Period and e-shop filters.'),
        caveats=_m(
            'Εξαρτάται από την ποιότητα της διεύθυνσης παράδοσης. Ασυνεπής γραφή πόλης σπάει το ranking.',
            'Depends on delivery-address quality. Inconsistent city spelling breaks the ranking.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_top_fulfilment',
        keys=('Κύριο μοντέλο εκτέλεσης', 'Top fulfilment model', 'Top κατάστημα εκτέλεσης'),
        title=_m('Κύριο Μοντέλο / Σημείο Εκτέλεσης', 'Top Fulfilment Model / Site'),
        what=_m(
            'Από πού εκτελείται κυρίως το e-shop — ποιο μοντέλο ή φυσικό σημείο σηκώνει τον περισσότερο όγκο.',
            'Where the e-shop is mostly fulfilled from — which model or physical site carries the most volume.',
        ),
        formula=_m(
            'Επιλέγεται το μοντέλο/σημείο με το μεγαλύτερο πλήθος παραγγελιών στο διάστημα.',
            'The model/site with the highest order count in the period is selected.',
        ),
        source=_m('Ρύθμιση eshop fulfillment του tenant πάνω στο κύκλωμα Πωλήσεων.', 'Tenant e-shop fulfilment configuration over the Sales stream.'),
        filters=_m('Περίοδος και φίλτρα e-shop.', 'Period and e-shop filters.'),
        caveats=_m(
            'Απαιτεί σωστή αντιστοίχιση σειρών/αποθηκών στη ρύθμιση fulfillment του tenant.',
            'Requires correct series/warehouse mapping in the tenant fulfilment configuration.',
        ),
        circuit='eshop-analysis',
    ),
    Kpi(
        id='eshop_top_payment',
        keys=('Κύριος τρόπος πληρωμής', 'Top payment method'),
        title=_m('Κύριος Τρόπος Πληρωμής', 'Top Payment Method'),
        what=_m('Ο τρόπος πληρωμής με τις περισσότερες παραγγελίες στο διάστημα.', 'The payment method with the most orders in the period.'),
        formula=_m('Επιλέγεται ο τρόπος πληρωμής με το μεγαλύτερο count παραγγελιών.', 'The payment method with the highest order count is selected.'),
        source=_m('Κύκλωμα Πωλήσεων, κανάλι e-shop.', 'Sales stream, e-shop channel.'),
        filters=_m('Περίοδος και φίλτρα e-shop.', 'Period and e-shop filters.'),
        caveats=_m(
            'Υψηλό μερίδιο αντικαταβολής αυξάνει άμεσα τα «Έξοδα Αντικαταβολής».',
            'A high COD share directly raises "Cash-on-Delivery Cost".',
        ),
        circuit='eshop-analysis',
    ),

    # ======================= Τιμολόγηση ====================================
    Kpi(
        id='pc_items_scope',
        keys=('Είδη στο επιλεγμένο scope', 'Κωδικοί ειδών'),
        title=_m('Είδη στο Scope', 'Items in Scope'),
        what=_m('Πόσα είδη συμμετέχουν στην ανάλυση τιμών με τα τρέχοντα φίλτρα.', 'How many items take part in the price analysis under current filters.'),
        formula=_m('Είδη στο Scope = count(distinct κωδικών ειδών μετά τα φίλτρα).', 'Items in Scope = count(distinct item codes after filters).'),
        source=_m('Αρχείο ειδών + κύκλωμα Αγορών + snapshot αποθήκης.', 'Item master + Purchases stream + warehouse snapshot.'),
        filters=_m('Προμηθευτής, brand, κατηγορία, ομάδα και αναζήτηση είδους.', 'Supplier, brand, category, group and item search.'),
        caveats=_m(
            'Είδη χωρίς αγορές στο διάστημα δεν έχουν τιμή κτήσης και μένουν εκτός των υπολογισμών περιθωρίου.',
            'Items with no purchases in the period have no acquisition price and stay out of margin calculations.',
        ),
        circuit='price-control',
    ),
    Kpi(
        id='pc_acquisition_price',
        keys=('Τιμή Κτήσης', 'Acquisition price'),
        title=_m('Τιμή Κτήσης', 'Acquisition Price'),
        what=_m(
            'Η πραγματική σταθμισμένη τιμή κτήσης του είδους — τι σου κοστίζει τελικά ένα τεμάχιο.',
            'The real weighted acquisition price of the item — what one unit actually costs you.',
        ),
        formula=_m(
            'Τιμή Κτήσης = σύνολο καθαρής αξίας αγορών / σύνολο ποσοτήτων αγορών. Οι δωρεάν ποσότητες με μηδενική αξία συμμετέχουν στην ποσότητα.',
            'Acquisition Price = total net purchase value / total purchased quantity. Free goods at zero value still count in quantity.',
        ),
        source=_m('Κύκλωμα Αγορών, γραμμές παραστατικών.', 'Purchases stream, document lines.'),
        filters=_m('Περίοδος αγορών, προμηθευτής και φίλτρα ειδών.', 'Purchase period, supplier and item filters.'),
        caveats=_m(
            'ΔΕΝ είναι η τελευταία τιμή αγοράς — είναι σταθμισμένος μέσος όρος όλης της περιόδου. Αν άλλαξες προμηθευτή πρόσφατα, διάλεξε στενότερη περίοδο.',
            'It is NOT the last purchase price — it is a weighted average over the whole period. If you recently switched supplier, narrow the period.',
        ),
        circuit='price-control',
    ),
    Kpi(
        id='pc_retail_margin',
        keys=('Μέσο Περιθώριο Λιανικής', 'Τρέχον Περιθώριο Λιανικής', 'Retail margin'),
        title=_m('Περιθώριο Λιανικής', 'Retail Margin'),
        what=_m(
            'Το περιθώριο που προκύπτει από την καθαρή λιανική και την καθαρή τιμή κτήσης.',
            'Margin derived from net retail price and net acquisition price.',
        ),
        formula=_m(
            'Περιθώριο Λιανικής = ((Καθαρή Λιανική - Τιμή Κτήσης) / Καθαρή Λιανική) * 100, όπου Καθαρή Λιανική = Λιανική με ΦΠΑ / (1 + ΦΠΑ%).',
            'Retail Margin = ((Net Retail - Acquisition Price) / Net Retail) * 100, where Net Retail = Retail incl. VAT / (1 + VAT%).',
        ),
        source=_m('Λιανική από SoftOne (MTRL.PRICER) μέσω inventory snapshot· κόστος από τις αγορές.', 'Retail from SoftOne (MTRL.PRICER) via the inventory snapshot; cost from purchases.'),
        filters=_m('Στόχος περιθωρίου, προμηθευτής και φίλτρα ειδών.', 'Target margin, supplier and item filters.'),
        caveats=_m(
            'Ο υπολογισμός γίνεται σε καθαρές τιμές, αλλά οι τιμές εμφανίζονται με ΦΠΑ για να συγκρίνονται με το ράφι. Αν λείπει η λιανική, έλεγξε τον τελευταίο συγχρονισμό αποθέματος.',
            'Computed on net prices while displayed prices include VAT so they match the shelf. If retail is missing, check the last stock sync.',
        ),
        circuit='price-control',
    ),
    Kpi(
        id='pc_target_price',
        keys=('Τιμή Στόχου', 'Στόχος Μικτού Περιθωρίου', 'Target price'),
        title=_m('Τιμή Στόχου', 'Target Price'),
        what=_m(
            'Η ελάχιστη τελική λιανική με ΦΠΑ που χρειάζεται για να πιάσεις τον στόχο μεικτού περιθωρίου.',
            'The minimum final retail price incl. VAT required to hit the target gross margin.',
        ),
        formula=_m(
            'Τιμή Στόχου = (Τιμή Κτήσης / (1 - Στόχος%)) * (1 + ΦΠΑ%). Παράδειγμα: κόστος 13,48 με στόχο 25% και ΦΠΑ 24% δίνει 13,48 / 0,75 * 1,24 = 22,29.',
            'Target Price = (Acquisition Price / (1 - Target%)) * (1 + VAT%). Example: cost 13.48 with a 25% target and 24% VAT gives 13.48 / 0.75 * 1.24 = 22.29.',
        ),
        source=_m('Τιμή κτήσης από αγορές, ΦΠΑ από το αρχείο ειδών.', 'Acquisition price from purchases, VAT from the item master.'),
        filters=_m('Ο στόχος μεικτού περιθωρίου που ορίζεις πάνω στη σελίδα.', 'The target gross margin you set on the page.'),
        caveats=_m(
            'Ο στόχος είναι περιθώριο πάνω στην τιμή πώλησης, όχι markup πάνω στο κόστος. Γι\' αυτό διαιρούμε με (1 - στόχος) και δεν πολλαπλασιάζουμε με (1 + στόχος).',
            'The target is a margin on the selling price, not a markup on cost. That is why we divide by (1 - target) rather than multiply by (1 + target).',
        ),
        circuit='price-control',
    ),
    Kpi(
        id='pc_above_below',
        keys=('Πάνω από Προβλ.', 'Κάτω από Προβλ.', 'Πάνω από Προβλεπόμενη', 'Κάτω από Προβλεπόμενη'),
        title=_m('Πάνω / Κάτω από την Τιμή Στόχου', 'Above / Below Target Price'),
        what=_m(
            'Πόσα είδη έχουν λιανική πάνω ή κάτω από την τιμή στόχου. Με κλικ ανοίγει αναλυτική λίστα ειδών.',
            'How many items are priced above or below the target price. Clicking opens the item-level list.',
        ),
        formula=_m(
            'Πάνω = count(είδη όπου Τιμή Λιανικής >= Τιμή Στόχου). Κάτω = count(είδη όπου Τιμή Λιανικής < Τιμή Στόχου).',
            'Above = count(items where Retail >= Target). Below = count(items where Retail < Target).',
        ),
        source=_m('Λιανική SoftOne έναντι υπολογισμένης τιμής στόχου.', 'SoftOne retail price against the computed target price.'),
        filters=_m('Ο στόχος περιθωρίου και τα φίλτρα ειδών.', 'The margin target and item filters.'),
        caveats=_m(
            '«Κάτω από την τιμή στόχου» δεν σημαίνει αυτόματα ζημιά — σημαίνει ότι δεν πιάνεις τον στόχο. Έλεγξε ανταγωνισμό πριν αλλάξεις τιμή.',
            '"Below target" does not automatically mean a loss — it means you are missing the target. Check competition before repricing.',
        ),
        circuit='price-control',
    ),

    # ======================= Αναπλήρωση / FNR ==============================
    Kpi(
        id='fnr_worksheet_rows',
        keys=('Γραμμές worksheet', 'Γραμμές Worksheet', 'Worksheet rows'),
        title=_m('Γραμμές Worksheet', 'Worksheet Rows'),
        what=_m('Πόσα είδη συμμετέχουν στο αποτέλεσμα μετά τα φίλτρα.', 'How many items take part in the result after filters.'),
        formula=_m('Γραμμές Worksheet = count(γραμμών μετά τα φίλτρα).', 'Worksheet Rows = count(rows after filters).'),
        source=_m('Υπολογισμός FNR πάνω σε πωλήσεις, απόθεμα και αναμενόμενα.', 'FNR computation over sales, stock and expected receipts.'),
        filters=_m('Φαρμακείο/σημείο, κατηγορίες, προμηθευτής και αναζήτηση είδους.', 'Site, categories, supplier and item search.'),
        caveats=_m(
            'Τα φίλτρα εφαρμόζονται ΠΡΙΝ τον υπολογισμό των KPI, οπότε όλα τα νούμερα αφορούν μόνο το επιλεγμένο scope.',
            'Filters are applied BEFORE the KPIs are computed, so every number refers only to the selected scope.',
        ),
        circuit='fnr',
    ),
    Kpi(
        id='fnr_order_rows',
        keys=('Γραμμές παραγγελίας', 'Order rows'),
        title=_m('Γραμμές Παραγγελίας', 'Order Rows'),
        what=_m('Πόσα είδη κατέληξαν με τελική προτεινόμενη ποσότητα παραγγελίας.', 'How many items ended up with a final suggested order quantity.'),
        formula=_m('Γραμμές Παραγγελίας = count(γραμμών όπου supplier_order_qty > 0).', 'Order Rows = count(rows where supplier_order_qty > 0).'),
        source=_m('Υπολογισμός FNR.', 'FNR computation.'),
        filters=_m('Παράμετροι target stock / overstock και τα φίλτρα σελίδας.', 'Target stock / overstock parameters and page filters.'),
        caveats=_m(
            'Μικρότερο από τις «Γραμμές Worksheet»: τα είδη που καλύπτονται ήδη δεν παραγγέλνονται.',
            'Lower than "Worksheet Rows": items already covered are not ordered.',
        ),
        circuit='fnr',
    ),
    Kpi(
        id='fnr_order_qty',
        keys=('Ποσότητα παραγγελίας', 'Order quantity'),
        title=_m('Ποσότητα Παραγγελίας', 'Order Quantity'),
        what=_m('Η συνολική ποσότητα που προτείνεται προς παραγγελία.', 'Total quantity suggested for ordering.'),
        formula=_m('Ποσότητα Παραγγελίας = άθροισμα supplier_order_qty.', 'Order Quantity = sum of supplier_order_qty.'),
        source=_m('Υπολογισμός FNR.', 'FNR computation.'),
        filters=_m('Target weeks, overstock weeks, MOQ και φίλτρα σελίδας.', 'Target weeks, overstock weeks, MOQ and page filters.'),
        caveats=_m(
            'Το Vendor MOQ μπορεί να ανεβάσει την ποσότητα πάνω από την καθαρή ανάγκη. Το BI δεν κρατά backorders: μετασχηματισμένη παραγγελία θεωρείται κλειστή.',
            'Vendor MOQ can push the quantity above the pure need. BI does not track backorders: a transformed order is treated as closed.',
        ),
        circuit='fnr',
    ),
    Kpi(
        id='fnr_order_value',
        keys=('Αξία παραγγελίας', 'Order value'),
        title=_m('Αξία Παραγγελίας', 'Order Value'),
        what=_m('Η εκτιμώμενη αξία της προτεινόμενης παραγγελίας.', 'Estimated value of the suggested order.'),
        formula=_m('Αξία Παραγγελίας = άθροισμα(supplier_order_qty * τιμή αγοράς).', 'Order Value = sum(supplier_order_qty * purchase price).'),
        source=_m('Υπολογισμός FNR με τιμές από το κύκλωμα Αγορών.', 'FNR computation with prices from the Purchases stream.'),
        filters=_m('Τα φίλτρα και οι παράμετροι της σελίδας.', 'Page filters and parameters.'),
        caveats=_m(
            'Είδη χωρίς τιμή αγοράς μπαίνουν στο Data Quality και δεν αποτιμώνται, οπότε η αξία υποεκτιμάται.',
            'Items with no purchase price land in Data Quality and are not valued, so the total is understated.',
        ),
        circuit='fnr',
    ),
    Kpi(
        id='fnr_items_in_need',
        keys=('Είδη με ανάγκη', 'Προϊόντα με Ανάγκη', 'Items in need'),
        title=_m('Είδη με Ανάγκη', 'Items in Need'),
        what=_m('Πόσα είδη βρίσκονται κάτω από το target stock τους.', 'How many items sit below their target stock.'),
        formula=_m('Είδη με Ανάγκη = count(γραμμών όπου need > 0).', 'Items in Need = count(rows where need > 0).'),
        source=_m('Απόθεμα, πωλήσεις και αναμενόμενα από προμηθευτές.', 'Stock, sales and expected supplier receipts.'),
        filters=_m('Target weeks, ελάχιστο απόθεμα και φίλτρα σελίδας.', 'Target weeks, minimum stock and page filters.'),
        caveats=_m(
            'Πριν αποδεχτείς πρόταση, έλεγξε τις ανοιχτές παραγγελίες προμηθευτών — ίσως το εμπόρευμα είναι ήδη καθ\' οδόν.',
            'Before accepting a suggestion, check open supplier orders — the goods may already be in transit.',
        ),
        circuit='fnr',
    ),
    Kpi(
        id='weeks_of_stock',
        keys=('Weeks of stock', 'Εβδομάδες κάλυψης', 'Κάλυψη αποθέματος'),
        title=_m('Εβδομάδες Κάλυψης', 'Weeks of Stock'),
        what=_m('Πόσες εβδομάδες καλύπτει το τρέχον διαθέσιμο απόθεμα με τον σημερινό ρυθμό πώλησης.', 'How many weeks current stock covers at the current sales rate.'),
        formula=_m(
            'Weeks of Stock = διαθέσιμο απόθεμα / max(μέσος εβδομαδιαίος ρυθμός πώλησης, 1).',
            'Weeks of Stock = available stock / max(average weekly sales rate, 1).',
        ),
        source=_m('Snapshot αποθήκης και κύκλωμα Πωλήσεων.', 'Warehouse snapshot and Sales stream.'),
        filters=_m('Σημείο, κατηγορία, προμηθευτής και παράθυρο υπολογισμού ρυθμού.', 'Site, category, supplier and the rate window.'),
        caveats=_m(
            'Είδη χωρίς πωλήσεις δίνουν τεράστια ή άπειρη κάλυψη — αγνόησέ τα και δες τα στο Destocking. Ο ρυθμός είναι ιστορικός, δεν προβλέπει εποχικότητα.',
            'Items with no sales yield huge or infinite coverage — ignore those and review them in Destocking. The rate is historical and does not forecast seasonality.',
        ),
        circuit='fnr',
    ),

    # ======================= Sell out ======================================
    Kpi(
        id='sellout_sales',
        keys=('Πωλήσεις sellout', 'Πωλήσεις'),
        title=_m('Πωλήσεις (Sell Out)', 'Sales (Sell Out)'),
        what=_m('Η αξία πωλήσεων των ειδών που περιλαμβάνει η αναφορά sell out.', 'Sales value of the items included in the sell-out report.'),
        formula=_m('Πωλήσεις = άθροισμα καθαρής αξίας πωλήσεων των επιλεγμένων ειδών στο [Από, Έως].', 'Sales = sum of net sales value for the selected items within [From, To].'),
        source=_m('Κύκλωμα Πωλήσεων περιορισμένο στο scope της αναφοράς.', 'Sales stream limited to the report scope.'),
        filters=_m('Περίοδος, προμηθευτής, κατηγορία και action κατηγορία.', 'Period, supplier, category and action category.'),
        caveats=_m(
            'Η αναφορά αφορά συγκεκριμένο scope ειδών — δεν είναι ο συνολικός τζίρος της επιχείρησης.',
            'The report covers a specific item scope — it is not the company\'s total turnover.',
        ),
        circuit='sellout',
    ),

    # ======================= Σύνολα παραστατικών ===========================
    #  These four are the footer totals of every document stream. They are the
    #  numbers users actually reconcile against SoftOne, so the split between
    #  them matters more than any single one.
    Kpi(
        id='doc_count',
        keys=('Παραστατικά', 'Documents'),
        title=_m('Παραστατικά', 'Documents'),
        what=_m('Πόσα παραστατικά περιλαμβάνει η λίστα με τα τρέχοντα φίλτρα.', 'How many documents the list contains under the current filters.'),
        formula=_m(
            'Παραστατικά = count(distinct παραστατικό) στο [Από, Έως].',
            'Documents = count(distinct document) within [From, To].',
        ),
        source=_m('Το κύκλωμα παραστατικών της σελίδας.', 'The document stream of the current page.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Μετράει παραστατικά, όχι γραμμές. Αν το πλήθος συμφωνεί με το SoftOne αλλά οι αξίες όχι, το θέμα είναι στα έξοδα ή στον ΦΠΑ.',
            'Counts documents, not lines. If the count matches SoftOne but the values do not, the difference is in charges or VAT.',
        ),
        circuit='sales-documents',
    ),
    Kpi(
        id='doc_net_value',
        keys=('Καθαρή Αξία', 'Net value'),
        title=_m('Καθαρή Αξία', 'Net Value'),
        what=_m('Η αξία των γραμμών χωρίς έξοδα και χωρίς ΦΠΑ.', 'Line value excluding charges and VAT.'),
        formula=_m(
            'Καθαρή Αξία = άθροισμα total_net_value των παραστατικών στο [Από, Έως].',
            'Net Value = sum of total_net_value over documents within [From, To].',
        ),
        source=_m('Το κύκλωμα παραστατικών της σελίδας.', 'The document stream of the current page.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Αυτό είναι το μέτρο που πρέπει να συγκρίνεις με «καθαρή αξία» στο SoftOne — όχι με το γενικό σύνολο.',
            'This is the measure to compare against SoftOne "net value" — not against the grand total.',
        ),
        circuit='sales-documents',
    ),
    Kpi(
        id='doc_expenses',
        keys=('Έξοδα', 'Έξοδα Παραστατικού', 'Document charges'),
        title=_m('Έξοδα Παραστατικού', 'Document Charges'),
        what=_m(
            'Σύνολο εξόδων παραστατικού, όπως μεταφορικά, αντικαταβολή ή συσκευασία.',
            'Total document charges such as shipping, cash-on-delivery or packaging.',
        ),
        formula=_m(
            'Έξοδα = άθροισμα total_expenses_value από την κεφαλίδα/ανάλυση εξόδων του παραστατικού.',
            'Charges = sum of total_expenses_value from the document header / charge breakdown.',
        ),
        source=_m('Κεφαλίδα παραστατικού.', 'Document header.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Τα έξοδα ΔΕΝ είναι πωλούμενο είδος: δεν έχουν ποσότητα και δεν μπαίνουν στην ανάλυση ειδών. Μετράνε μία φορά ανά παραστατικό, όχι ανά γραμμή.',
            'Charges are NOT a sold item: they carry no quantity and never enter item analysis. They count once per document, not per line.',
        ),
        circuit='sales-documents',
    ),
    Kpi(
        id='doc_vat',
        keys=('ΦΠΑ', 'VAT'),
        title=_m('ΦΠΑ', 'VAT'),
        what=_m('Ο ΦΠΑ των παραστατικών της λίστας.', 'VAT on the documents in the list.'),
        formula=_m('ΦΠΑ = άθροισμα total_vat_value στο [Από, Έως].', 'VAT = sum of total_vat_value within [From, To].'),
        source=_m('Κεφαλίδα παραστατικού.', 'Document header.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Όλα τα υπόλοιπα KPI του BI είναι καθαρά. Ο ΦΠΑ εμφανίζεται μόνο εδώ, για συμφωνία με το γενικό σύνολο.',
            'Every other BI metric is net. VAT appears only here, so the grand total can be reconciled.',
        ),
        circuit='sales-documents',
    ),
    Kpi(
        id='doc_total',
        keys=('Σύνολο', 'Γενικό Σύνολο', 'Grand total'),
        title=_m('Γενικό Σύνολο', 'Grand Total'),
        what=_m('Η τελική αξία των παραστατικών, όπως θα τη δεις και στο SoftOne.', 'Final document value, as you would see it in SoftOne.'),
        formula=_m('Γενικό Σύνολο = Καθαρή Αξία + Έξοδα + ΦΠΑ.', 'Grand Total = Net Value + Charges + VAT.'),
        source=_m('Κεφαλίδα παραστατικού.', 'Document header.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Είναι το μόνο μέγεθος του BI που περιέχει ΦΠΑ. Μην το συγκρίνεις με τα KPI τζίρου των dashboards, που είναι καθαρά.',
            'It is the only BI figure that includes VAT. Do not compare it with dashboard turnover KPIs, which are net.',
        ),
        circuit='sales-documents',
    ),
    Kpi(
        id='doc_average',
        keys=('Μέσο Παραστατικό', 'Average document'),
        title=_m('Μέσο Παραστατικό', 'Average Document'),
        what=_m('Η μέση αξία ανά παραστατικό στο διάστημα.', 'Average value per document in the period.'),
        formula=_m(
            'Μέσο Παραστατικό = (Καθαρή Αξία + Έξοδα) / πλήθος παραστατικών.',
            'Average Document = (Net Value + Charges) / number of documents.',
        ),
        source=_m('Το κύκλωμα παραστατικών της σελίδας.', 'The document stream of the current page.'),
        filters=_PERIOD_FILTERS,
        caveats=_m(
            'Τα πιστωτικά μετράνε ως παραστατικά με αρνητική αξία και τραβούν τον μέσο όρο κάτω.',
            'Credit notes count as documents with negative value and drag the average down.',
        ),
        circuit='sales-analytics',
    ),
    Kpi(
        id='open_supplier_orders',
        keys=('Ανοιχτές Παραγγελίες', 'Open supplier orders'),
        title=_m('Ανοιχτές Παραγγελίες', 'Open Supplier Orders'),
        what=_m(
            'Παραγγελίες προς προμηθευτές που δεν έχουν μετασχηματιστεί σε παραστατικό αγοράς.',
            'Supplier orders not yet transformed into a purchase document.',
        ),
        formula=_m(
            'Ανοιχτές Παραγγελίες = count(distinct παραστατικών παραγγελίας χωρίς μετασχηματισμό) στο lookback διάστημα.',
            'Open Supplier Orders = count(distinct order documents with no transformation) within the lookback window.',
        ),
        source=_m('Κύκλωμα Παραγγελιών Προμηθευτών.', 'Supplier orders stream.'),
        filters=_m(
            'Προμηθευτής, παραστατικό, «μόνο ανοιχτές / όλες» και το lookback ημερών από τις Ρυθμίσεις.',
            'Supplier, document, "open only / all" and the lookback days from Settings.',
        ),
        caveats=_m(
            'Μερική παράδοση κλείνει την παραγγελία: αν έγινε μετασχηματισμός, θεωρείται κλειστή ακόμη κι αν ήρθε μέρος. Το BI δεν κρατά backorders.',
            'A partial delivery closes the order: once transformed it counts as closed even if only part arrived. BI does not track backorders.',
        ),
        circuit='supplier-orders',
    ),
    Kpi(
        id='aging_buckets',
        keys=('Aging', 'Ενηλικίωση', 'Aging buckets'),
        title=_m('Aging (Ενηλικίωση Υπολοίπων)', 'Aging Buckets'),
        what=_m(
            'Πόσο παλιά είναι τα ανοικτά υπόλοιπα, κατανεμημένα σε ζώνες καθυστέρησης.',
            'How old open balances are, distributed into overdue bands.',
        ),
        formula=_m(
            'Κάθε ανοικτό υπόλοιπο τοποθετείται σε ζώνη με βάση τις ημέρες από την ημερομηνία λήξης: μη ληξιπρόθεσμο, 1-30, 31-60, 61-90, 90+.',
            'Each open balance is bucketed by days past its due date: not due, 1-30, 31-60, 61-90, 90+.',
        ),
        source=_m('Κυκλώματα Υπολοίπων Πελατών και Προμηθευτών.', 'Customer and supplier balances streams.'),
        filters=_m('Πελάτης / προμηθευτής και αναζήτηση.', 'Customer / supplier and search.'),
        caveats=_m(
            'Χωρίς ημερομηνία λήξης στο παραστατικό, το υπόλοιπο δεν μπορεί να ενηλικιωθεί και μένει στο «μη ληξιπρόθεσμο».',
            'Without a due date on the document a balance cannot be aged and stays in "not due".',
        ),
        circuit='customer-balances',
    ),
    Kpi(
        id='sell_through',
        keys=('Sell Through', 'Sell-through'),
        title=_m('Sell Through', 'Sell Through'),
        what=_m('Πόσο γρήγορα φεύγει το απόθεμα που έφερες.', 'How quickly the stock you brought in sells out.'),
        formula=_m(
            'Sell Through = Πωλήσεις / (Πωλήσεις + Απόθεμα) * 100.',
            'Sell Through = Sales / (Sales + Stock) * 100.',
        ),
        source=_m('Κύκλωμα Πωλήσεων και snapshot αποθήκης.', 'Sales stream and warehouse snapshot.'),
        filters=_m('Περίοδος, προμηθευτής, κατηγορία και είδος.', 'Period, supplier, category and item.'),
        caveats=_m(
            'Χρειάζεται και τα δύο σκέλη: είδος χωρίς απόθεμα δίνει 100% χωρίς να σημαίνει επιτυχία — μπορεί απλώς να έχει εξαντληθεί.',
            'Needs both terms: an item with no stock returns 100% without meaning success — it may simply be out of stock.',
        ),
        circuit='sellout',
    ),
    Kpi(
        id='days_of_supply',
        keys=('Days of Supply', 'Ημέρες Κάλυψης'),
        title=_m('Ημέρες Κάλυψης', 'Days of Supply'),
        what=_m('Πόσες ημέρες καλύπτει το τρέχον απόθεμα.', 'How many days current stock covers.'),
        formula=_m('Ημέρες Κάλυψης = απόθεμα / μέση ημερήσια πώληση.', 'Days of Supply = stock / average daily sales.'),
        source=_m('Snapshot αποθήκης και κύκλωμα Πωλήσεων.', 'Warehouse snapshot and Sales stream.'),
        filters=_m('Περίοδος υπολογισμού ρυθμού, σημείο και φίλτρα ειδών.', 'The rate window, site and item filters.'),
        caveats=_m(
            'Είδη χωρίς πωλήσεις δίνουν άπειρη κάλυψη. Ο ρυθμός είναι ιστορικός και δεν προβλέπει εποχική ζήτηση.',
            'Items with no sales yield infinite coverage. The rate is historical and does not forecast seasonal demand.',
        ),
        circuit='sellout',
    ),
    Kpi(
        id='gmroi',
        keys=('GMROI',),
        title=_m('GMROI', 'GMROI'),
        what=_m(
            'Πόσο μικτό κέρδος αποδίδει κάθε ευρώ που έχεις δεσμευμένο σε απόθεμα.',
            'How much gross profit each euro tied up in stock returns.',
        ),
        formula=_m('GMROI = Μικτό Κέρδος / Κόστος Αποθέματος.', 'GMROI = Gross Profit / Inventory Cost.'),
        source=_m('Κύκλωμα Πωλήσεων και αποτίμηση αποθέματος.', 'Sales stream and inventory valuation.'),
        filters=_m('Περίοδος, προμηθευτής, κατηγορία και είδος.', 'Period, supplier, category and item.'),
        caveats=_m(
            'Τιμή κάτω από 1 σημαίνει ότι το είδος δεν αποδίδει όσο κοστίζει να το κρατάς. Είναι ο πιο χρήσιμος δείκτης για αποφάσεις destocking.',
            'A value below 1 means the item returns less than it costs to hold. It is the most useful metric for destocking decisions.',
        ),
        circuit='sellout',
    ),

    # ======================= Insights / Call center / αγορά ================
    Kpi(
        id='insights_count',
        keys=('Πλήθος Insights', 'Insights'),
        title=_m('Πλήθος Insights', 'Number of Insights'),
        what=_m('Πόσες ενεργές προειδοποιήσεις υπάρχουν με τα τρέχοντα φίλτρα.', 'How many active alerts exist under the current filters.'),
        formula=_m('Πλήθος Insights = count(insights) μετά τα φίλτρα.', 'Insights = count(insights) after filters.'),
        source=_m('Μηχανή κανόνων BI πάνω σε όλα τα κυκλώματα.', 'BI rules engine across all streams.'),
        filters=_m('Περίοδος, κατηγορία, σοβαρότητα και κατάσταση χειρισμού.', 'Period, category, severity and handling status.'),
        caveats=_m(
            'Το insight είναι ένδειξη προς έλεγχο, όχι λογιστικό αποτέλεσμα. Πολλά insights δεν σημαίνουν πρόβλημα — δες πρώτα τη σοβαρότητα.',
            'An insight is a signal to check, not an accounting result. Many insights do not imply trouble — look at severity first.',
        ),
        circuit='insights',
    ),
    Kpi(
        id='insights_severity',
        keys=('Σοβαρότητα', 'Severity'),
        title=_m('Σοβαρότητα', 'Severity'),
        what=_m('Πόσο κρίσιμη είναι η ένδειξη και με ποια σειρά πρέπει να τη δεις.', 'How critical the signal is and in what order to address it.'),
        formula=_m(
            'Η σοβαρότητα ορίζεται από τον κανόνα που παρήγαγε το insight (info, warning, high).',
            'Severity is set by the rule that produced the insight (info, warning, high).',
        ),
        source=_m('Ορισμός κανόνα στη μηχανή insights.', 'Rule definition in the insights engine.'),
        filters=_m('Φίλτρο σοβαρότητας στη σελίδα.', 'The severity filter on the page.'),
        caveats=_m(
            'Τα όρια σοβαρότητας είναι παραμετρικά ανά tenant. Αν όλα βγαίνουν «high», τα όρια θέλουν ρύθμιση.',
            'Severity thresholds are per-tenant. If everything reads "high", the thresholds need tuning.',
        ),
        circuit='insights',
    ),
    Kpi(
        id='cc_calls',
        keys=('Κλήσεις', 'Calls'),
        title=_m('Κλήσεις', 'Calls'),
        what=_m('Πόσες κλήσεις καταγράφηκαν στο επιλεγμένο διάστημα.', 'How many calls were logged in the selected period.'),
        formula=_m('Κλήσεις = count(εγγραφών κλήσης) στο [Από, Έως].', 'Calls = count(call records) within [From, To].'),
        source=_m('Σύνδεση με το τηλεφωνικό κέντρο.', 'Phone-system connector.'),
        filters=_m('Περίοδος, κατεύθυνση κλήσης, ζώνη ώρας και χειριστής.', 'Period, call direction, time band and agent.'),
        caveats=_m(
            'Αν λείπουν ημέρες, το πρόβλημα είναι στη σύνδεση με το τηλεφωνικό κέντρο, όχι στα δεδομένα.',
            'Missing days point to the phone-system connection, not to the data.',
        ),
        circuit='call-center',
    ),
    Kpi(
        id='cc_avg_wait',
        keys=('Μέσος Χρόνος Αναμονής', 'Average wait time'),
        title=_m('Μέσος Χρόνος Αναμονής', 'Average Wait Time'),
        what=_m('Πόση ώρα περιμένει κατά μέσο όρο ένας πελάτης μέχρι να απαντηθεί.', 'How long a caller waits on average before being answered.'),
        formula=_m(
            'Μέσος Χρόνος Αναμονής = άθροισμα χρόνου αναμονής / πλήθος κλήσεων, υπολογισμένος ανά περίοδο από τις ίδιες τις κλήσεις.',
            'Average Wait = total wait time / number of calls, recomputed per period from the call rows themselves.',
        ),
        source=_m('Εγγραφές κλήσεων του τηλεφωνικού κέντρου.', 'Phone-system call rows.'),
        filters=_m('Περίοδος, ζώνη ώρας και κατεύθυνση.', 'Period, time band and direction.'),
        caveats=_m(
            'Ο μέσος όρος κρύβει τις αιχμές. Δες τον ανά ζώνη ώρας για να βρεις πότε χάνεις κλήσεις.',
            'The average hides peaks. Break it down by time band to find when calls are lost.',
        ),
        circuit='call-center',
    ),
    Kpi(
        id='cc_avg_talk',
        keys=('Μέσος Χρόνος Ομιλίας', 'Average talk time'),
        title=_m('Μέσος Χρόνος Ομιλίας', 'Average Talk Time'),
        what=_m('Πόσο διαρκεί κατά μέσο όρο μια απαντημένη κλήση.', 'How long an answered call lasts on average.'),
        formula=_m(
            'Μέσος Χρόνος Ομιλίας = άθροισμα διάρκειας ομιλίας / πλήθος απαντημένων κλήσεων.',
            'Average Talk Time = total talk duration / number of answered calls.',
        ),
        source=_m('Εγγραφές κλήσεων του τηλεφωνικού κέντρου.', 'Phone-system call rows.'),
        filters=_m('Περίοδος, ζώνη ώρας, κατεύθυνση και χειριστής.', 'Period, time band, direction and agent.'),
        caveats=_m(
            'Οι αναπάντητες κλήσεις δεν μπαίνουν στον παρονομαστή.',
            'Unanswered calls are excluded from the denominator.',
        ),
        circuit='call-center',
    ),
    Kpi(
        id='st_target',
        keys=('Στόχος', 'Target'),
        title=_m('Στόχος Συμφωνίας', 'Agreement Target'),
        what=_m('Το ποσό ή η ποσότητα που πρέπει να επιτευχθεί στη συμφωνία με τον προμηθευτή.', 'The amount or quantity to be achieved under the supplier agreement.'),
        formula=_m('Στόχος = η τιμή συμφωνίας που έχει καταχωρήσει ο χρήστης.', 'Target = the agreed value entered by the user.'),
        source=_m('Καταχώρηση συμφωνίας στο BI.', 'Agreement record in BI.'),
        filters=_m('Προμηθευτής και περίοδος ισχύος της συμφωνίας.', 'Supplier and the agreement validity period.'),
        caveats=_m(
            'Ο στόχος δεν έρχεται από το SoftOne — καταχωρείται εδώ. Λάθος περίοδος ισχύος δίνει λάθος πρόοδο.',
            'The target does not come from SoftOne — it is entered here. A wrong validity period yields wrong progress.',
        ),
        circuit='supplier-targets',
    ),
    Kpi(
        id='st_progress',
        keys=('Πρόοδος', 'Progress'),
        title=_m('Πρόοδος Συμφωνίας', 'Agreement Progress'),
        what=_m('Πόσο έχει καλυφθεί ο στόχος μέχρι σήμερα.', 'How much of the target has been covered so far.'),
        formula=_m('Πρόοδος = (πραγματοποιημένη αξία / στόχος) * 100.', 'Progress = (achieved value / target) * 100.'),
        source=_m('Κύκλωμα Αγορών περιορισμένο στα συμμετέχοντα είδη της συμφωνίας.', 'Purchases stream limited to the agreement\'s participating items.'),
        filters=_m('Τα συμμετέχοντα είδη και η περίοδος ισχύος.', 'Participating items and the validity period.'),
        caveats=_m(
            'Αν τα συμμετέχοντα είδη δεν είναι σωστά ορισμένα, η πρόοδος βγαίνει λάθος. Έλεγξέ τα πριν διαπραγματευτείς.',
            'If participating items are wrong the progress is wrong. Verify them before negotiating.',
        ),
        circuit='supplier-targets',
    ),
    Kpi(
        id='era_market_share',
        keys=('Μερίδιο Αξίας', 'Market share'),
        title=_m('Μερίδιο Αξίας', 'Value Market Share'),
        what=_m('Το μερίδιό μας στην αγορά του δείγματος που περιέχει το αρχείο.', 'Our share of the market covered by the uploaded sample.'),
        formula=_m(
            'Μερίδιο Αξίας = (δικές μας πωλήσεις / αξία αγοράς του δείγματος) * 100.',
            'Value Share = (our sales / sample market value) * 100.',
        ),
        source=_m('Εισαγόμενο αρχείο eRA διασταυρωμένο με τις δικές μας πωλήσεις μέσω barcode.', 'Imported eRA file matched to our sales by barcode.'),
        filters=_m('Κατηγορία, brand και φίλτρα του δείγματος.', 'Category, brand and sample filters.'),
        caveats=_m(
            'Ισχύει μόνο για το δείγμα και τον μήνα του αρχείου, όχι για όλη την αγορά. Λανθασμένα ή ελλιπή barcodes ρίχνουν τεχνητά το μερίδιο.',
            'Valid only for the file\'s sample and month, not the whole market. Wrong or missing barcodes artificially lower the share.',
        ),
        circuit='era-exploration-data',
    ),
    Kpi(
        id='era_add_suggestions',
        keys=('Προτάσεις Προσθήκης', 'Add suggestions'),
        title=_m('Προτάσεις Προσθήκης', 'Assortment Suggestions'),
        what=_m(
            'Είδη με αξιόλογο μερίδιο αγοράς που δεν υπάρχουν στη δική μας βάση.',
            'Items with meaningful market share that we do not carry.',
        ),
        formula=_m(
            'Προτάσεις = count(ειδών χωρίς αντιστοίχιση) πάνω από ένα ελάχιστο όριο σημαντικότητας αγοράς.',
            'Suggestions = count(unmatched items) above a minimum market-significance threshold.',
        ),
        source=_m('Εισαγόμενο αρχείο eRA έναντι του αρχείου ειδών μας.', 'Imported eRA file against our item master.'),
        filters=_m('Κατηγορία, brand και ελάχιστο όριο σημαντικότητας.', 'Category, brand and the significance threshold.'),
        caveats=_m(
            'Δεν προτείνεται κάθε είδος που λείπει, μόνο όσα έχουν σημαντικό μερίδιο. Έλεγξε την εποχικότητα από τον μήνα του αρχείου.',
            'Not every missing item is suggested, only those with meaningful share. Check seasonality via the file\'s month.',
        ),
        circuit='era-exploration-data',
    ),
    Kpi(
        id='iqvia_market_value',
        keys=('Αξία Αγοράς', 'Market value'),
        title=_m('Αξία Αγοράς', 'Market Value'),
        what=_m('Η συνολική αξία αγοράς για το δείγμα IQVIA του μήνα.', 'Total market value for the month\'s IQVIA sample.'),
        formula=_m('Αξία Αγοράς = άθροισμα VALUES του μήνα (πεδίο YYYYMM).', 'Market Value = sum of the month\'s VALUES field (YYYYMM).'),
        source=_m('Εισαγόμενα IQVIA BI tables — όχι απευθείας από το Excel.', 'Imported IQVIA BI tables — not read directly from Excel.'),
        filters=_m('Κατηγορία, κατασκευαστής, περιοχή και μήνας.', 'Category, manufacturer, territory and month.'),
        caveats=_m(
            'Κάθε αρχείο αφορά συγκεκριμένο μήνα. Για σωστές τάσεις πρέπει να ανεβαίνουν όλα τα μηνιαία αρχεία με συνέπεια.',
            'Each file covers one month. Consistent monthly uploads are required for trends to be meaningful.',
        ),
        circuit='iqvia',
    ),
    Kpi(
        id='iqvia_avg_price',
        keys=('Μέση Τιμή', 'Average price'),
        title=_m('Μέση Τιμή Αγοράς', 'Average Market Price'),
        what=_m('Η μέση τιμή ανά μονάδα στο δείγμα αγοράς.', 'Average price per unit across the market sample.'),
        formula=_m('Μέση Τιμή = Αξία Αγοράς / Μονάδες.', 'Average Price = Market Value / Units.'),
        source=_m('Εισαγόμενα IQVIA BI tables.', 'Imported IQVIA BI tables.'),
        filters=_m('Κατηγορία, κατασκευαστής, περιοχή και μήνας.', 'Category, manufacturer, territory and month.'),
        caveats=_m(
            'Είναι μέση τιμή αγοράς, όχι δική σου. Σύγκρινέ την με τη δική σου μέση τιμή πώλησης για να δεις αν είσαι πάνω ή κάτω από την αγορά.',
            'This is the market average, not yours. Compare it against your own average selling price to see where you stand.',
        ),
        circuit='iqvia',
    ),
)


@lru_cache(maxsize=4)
def catalog_for_lang(lang: Lang = 'el') -> list[dict[str, Any]]:
    """The catalog as plain dicts, ready for JSON or template rendering."""
    normalized = 'en' if str(lang).lower().startswith('en') else 'el'
    return [kpi.as_dict(normalized) for kpi in CATALOG]


@lru_cache(maxsize=4)
def catalog_by_circuit(lang: Lang = 'el') -> dict[str, list[dict[str, Any]]]:
    """KPIs grouped by the manual circuit they belong to."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in catalog_for_lang(lang):
        grouped.setdefault(entry['circuit'], []).append(entry)
    return grouped


def default_help(lang: Lang = 'el') -> dict[str, str]:
    """Shown when a card title matches nothing in the catalog."""
    if str(lang).lower().startswith('en'):
        return {
            'what': 'This metric summarises the selected business value for the active filters.',
            'formula': 'Calculated from aggregated data based on the selected date range and filters.',
            'source': 'The operational stream behind the current page.',
            'filters': 'The From / To period and every active filter on the page.',
            'caveats': 'Values are net of VAT and reflect data up to the last SoftOne sync.',
        }
    return {
        'what': 'Το KPI συνοψίζει την επιχειρηματική τιμή για τα ενεργά φίλτρα.',
        'formula': 'Υπολογίζεται από τα συγκεντρωτικά δεδομένα βάσει περιόδου και φίλτρων.',
        'source': 'Το επιχειρησιακό κύκλωμα πίσω από την τρέχουσα σελίδα.',
        'filters': 'Η περίοδος «Από / Έως» και κάθε ενεργό φίλτρο της σελίδας.',
        'caveats': 'Οι αξίες είναι καθαρές, χωρίς ΦΠΑ, και δείχνουν μέχρι τον τελευταίο συγχρονισμό SoftOne.',
    }
