import csv
from datetime import datetime

from beancount.core import data

import beangulp
from beancount_importers.bank_classifier import payee_to_account_mapping

from beangulp.importers.csvbase import Date, Amount, Column, Importer

CATEGORY_TO_ACCOUNT_MAPPING = {
    "Eating out": "Expenses:EatingOut",
    "Groceries": "Expenses:Groceries",
    "Shopping": "Expenses:Shopping",
    "Accommodation": "Expenses:Accommodation",
    "Bills": "Expenses:Bills",
    "Hobbies": "Expenses:Hobbies",
    "Wellness": "Expenses:Wellness",
    "Transport": "Expenses:Transport",
    "Travel": "Expenses:Travel",
    "Entertainment": "Expenses:Entertainment",
    "Donations": "Expenses:Donations",
}

TRANSACTIONS_CLASSIFIED_BY_ID = {}

UNCATEGORIZED_EXPENSES_ACCOUNT = "Expenses:FIXME"


def get_importer(account, currency, importer_params=None):
    params = importer_params if importer_params is not None else {}

    class MonzoBase(Importer):
        names = True

        def identify(self, filepath: str) -> bool:
            return filepath.endswith("csv")

        def extract(self, filepath, existing):
            entries = super().extract(filepath, existing)
            if not entries:
                return entries
            source_descs = {}
            for entry in existing or []:
                if not isinstance(entry, data.Transaction):
                    continue
                for posting in entry.postings:
                    meta = posting.meta
                    if not meta:
                        continue
                    source_desc = meta.get("source_desc")
                    if source_desc is None:
                        continue
                    for link in entry.links:
                        source_descs.setdefault(link, source_desc)
            if not source_descs:
                return entries
            out = []
            for entry in entries:
                if isinstance(entry, data.Transaction):
                    for link in entry.links:
                        source_desc = source_descs.get(link)
                        if source_desc is not None and entry.narration != source_desc:
                            entry = entry._replace(narration=source_desc)
                        break
                out.append(entry)
            return out

        def categorize(self, txn, row):
            payee = txn.payee
            description = txn.narration
            monzo_category: str = getattr(row, "category", "")

            if description == "Standing order" or description.startswith("Direct debit"):
                txn = txn._replace(tags=txn.tags.union(frozenset(['recurring'])))

            tags = [t[1:] for t in description.split(" ") if t.startswith('#')]
            if len(tags) > 0:
                txn = txn._replace(tags=txn.tags.union(frozenset(tags)))

            posting_account = None
            if txn.postings[0].units.number <= 0:
                # Expenses
                posting_account = payee_to_account_mapping.get(payee)

                # Default by category
                if not params.get("ignore_bank_categories"):
                    if not posting_account:
                        posting_account = CATEGORY_TO_ACCOUNT_MAPPING.get(
                            monzo_category, UNCATEGORIZED_EXPENSES_ACCOUNT
                        )
            else:
                if not params.get("ignore_bank_categories"):
                    if payee == "Savings Pot" or payee == "Savings Monzo Pot":
                        posting_account = "Assets:Monzo:Personal:Savings"

            if not posting_account:
                posting_account = UNCATEGORIZED_EXPENSES_ACCOUNT

            txn.postings.append(
                data.Posting(posting_account, -txn.postings[0].units, None, None, None, None)
            )

            txn.meta['source_desc'] = description
            return txn

        def _effective_narration(self, row):
            return row.narration

        def finalize(self, txn, row):
            # Don't need the active card checks
            if txn.postings[0].units.number == 0:
                return None
            narration = self._effective_narration(row)
            if narration and narration != txn.narration:
                txn = txn._replace(narration=narration)
            return self.categorize(txn, row)

    class MonzoImporter(MonzoBase):
        date = Date("Date", frmt="%d/%m/%Y")
        narration = Column("Description")
        payee = Column("Name")
        amount = Amount("Amount")
        currency = Column("Currency")
        category = Column("Category")
        link = Column("Transaction ID")

    class MonzoStatementImporter(MonzoBase):
        date = Date("Date", frmt="%d/%m/%Y")
        narration = Column("Description")
        notes = Column("Notes and #tags")
        payee = Column("Name")
        amount = Amount("Amount")
        currency = Column("Currency")
        category = Column("Category")
        link = Column("Transaction ID")

        def _effective_narration(self, row):
            return row.notes or row.narration

    csv_importer = MonzoImporter(account=account, currency=currency)
    statement_importer = MonzoStatementImporter(account=account, currency=currency)

    class MultiFormatMonzoImporter(beangulp.Importer):
        def _select(self, filepath):
            with open(filepath, encoding="utf8", newline="") as fd:
                reader = csv.reader(fd)
                try:
                    header = next(reader)
                except StopIteration:
                    return csv_importer
                names = {name.strip(): index for index, name in enumerate(header)}
                if "Notes and #tags" not in names:
                    return csv_importer
                date_col = names.get("Date")
                if date_col is None:
                    return csv_importer
                for row in reader:
                    if date_col < len(row):
                        raw = row[date_col].strip()
                        if raw:
                            try:
                                year = datetime.strptime(raw, "%d/%m/%Y").year
                            except ValueError:
                                continue
                            if year >= 2026:
                                return statement_importer
            return csv_importer

        def identify(self, filepath: str) -> bool:
            return self._select(filepath).identify(filepath)

        def account(self, filepath):
            return account

        def date(self, filepath):
            return self._select(filepath).date(filepath)

        def extract(self, filepath, existing):
            return self._select(filepath).extract(filepath, existing)

    return MultiFormatMonzoImporter()

if __name__ == "__main__":
    ingest = beangulp.Ingest([get_importer("Assets:Monzo:Cash", "GBP", {})], [])
    ingest()
