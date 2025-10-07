from functools import partial

import dateutil
from beancount.core import data

import beangulp
from beancount_importers.bank_classifier import payee_to_account_mapping
from beangulp.importers.csvbase import Date, Amount, CreditOrDebit, CSVReader, Column, Importer

TRANSACTIONS_CLASSIFIED_BY_PAYEE = {
    "ATM Withdrawal": "Assets:Physical:Cash",
    "O2": "Expenses:Bills:Phone",
}

UNCATEGORIZED_EXPENSES_ACCOUNT = "Expenses:FIXME"


def get_importer(account, currency, importer_params = None):
    class NationwideReader(Importer):
        date = Date(0, frmt="%d %b %Y")
        tx_type = Column(1)
        narration = Column(2)
        payee = Column(2)
        amount = CreditOrDebit(4, 3, subs={"[^\\d.]":""})
        balance = Amount(5, subs={"[^\\d.]":""})

        encoding = "iso-8859-1"
        header = 4
        names = True
        
        params = importer_params if importer_params is not None else {}
        my_account = account
        
        def identify(self, filepath: str) -> bool:
            return True 
            
        def categorize(self, params, txn, row):
            payee = txn.payee
            description = txn.narration

            if description == "Standing order" or description.startswith("Direct debit"):
                txn = txn._replace(tags=txn.tags.union(frozenset(['recurring'])))
                
            posting_account = UNCATEGORIZED_EXPENSES_ACCOUNT
            if description.startswith("Interest added"):
                accounts_parts = self.my_account.split(':')
                posting_account = 'Income:Uncategorized:' + ':'.join(accounts_parts[1:])
            else: 
                for mapped_payee, acct in (TRANSACTIONS_CLASSIFIED_BY_PAYEE | self.params.get('by_payee', {})).items():
                    if payee.startswith(mapped_payee):
                        posting_account = acct

            txn.postings.append(
                data.Posting(posting_account, -txn.postings[0].units, None, None, None, None)
            )
            
            return txn

            
        def finalize(self, txn, row):
            return self.categorize(self.params, txn, row)
        
    return NationwideReader(account=account, currency=currency)


if __name__ == "__main__":
    ingest = beangulp.Ingest([get_importer("Assets:Nationwide:Personal", "GBP", {
        "by_payee": {
            "ARTS EMERGENCY": "Expenses:Donations"
        }
    })], [])
    ingest()
