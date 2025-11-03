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


def get_importer(account, currency, importer_params):
    class MonzoImporter(Importer):
        date = Date("Date", frmt="%d/%m/%Y")
        narration = Column("Description")
        payee = Column("Name")
        amount = Amount("Amount")
        currency = Column("Currency")
        category = Column("Category")
        link = Column("Transaction ID")
        
        names = True

        params = importer_params if importer_params is not None else {}
        my_account = account
      
        def identify(self, filepath: str) -> bool:
            return filepath.endswith("csv") 

        def categorize(self, params, txn, row):
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
            
        def finalize(self, txn, row):
            # Don't need the active card checks 
            if txn.postings[0].units.number == 0:
                return None
            return self.categorize(self.params, txn, row)
    
    return MonzoImporter(account=account, currency=currency)

if __name__ == "__main__":
    ingest = beangulp.Ingest([get_importer("Assets:Monzo:Cash", "GBP", {})], [])
    ingest()
