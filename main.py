import os
from typing import List

from src.account_balance_calculation import AccountBalanceCalculation


def get_files() -> List[str]:
    files = os.listdir('data')
    return [f'data/{file}' for file in files]

def main():
    files = get_files()
    account_balance_calculation = AccountBalanceCalculation(files)
    customers_balance = account_balance_calculation.calculate_balance()
    account_balance_calculation.save_data(
        customers_balance,
        'balanco_cliente.csv'
        'data/'
    )
    consolidated_customers_balance = account_balance_calculation.calculate_consolidated_balance(
        customers_balance
    )
    account_balance_calculation.save_data(
        consolidated_customers_balance,
        'balanco_consolidado.csv',
        'data/'
    )

if __name__ == '__main__':
    main()