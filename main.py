import os
import functions
from tinkoff.invest import Client
from tinkoff.invest import (
    RequestError, PortfolioResponse,
    PositionsResponse, PortfolioPosition,
    AccessLevel, Operation, CandleInterval, )
import pandahouse

READ_TOKEN = os.environ.get('READ_TOKEN')

connection = {
    'host': 'http://localhost:18123',
    'database': 'tinkoff',
    'table': 'instruments',
}

q = 'SELECT * FROM tinkoff.instruments'
table = 'instruments'


def main():
    with Client(READ_TOKEN) as client:
        porfolio_manager = functions.PorfolioManager(client)
        # # Курс доллара
        # print(f'Текущий курс доллара: {porfolio_manager.get_usdrur()} р.')
        # # Cчета
        # accounts = porfolio_manager.get_accounts()
        # print(f'Актуальные счета: {accounts}')
        #
        # # Вывод актуальных позиций на кажом счете
        # portfolio = porfolio_manager.get_portfolio_df('2125540118')
        # print(portfolio.tail())
        #
        # # Операции по счету
        # operations = porfolio_manager.get_operations_df('2125540118')
        #
        # print(operations.tail())
        #
        # # Денежные позиции на счете
        # print(porfolio_manager.get_money_df('2125540118'))

        # Исторические данные по свечам
        information_parser = functions.InformationParser(client)

        # candles = information_parser.get_history_candles_df('BBG000HS77T5')

        # Обновление БД по свечам всех акций
        shares = pandahouse.read_clickhouse("SELECT figi FROM tinkoff.instruments WHERE instrument_type = 'share'", connection=connection)
        shares = shares['figi'].to_list()

        # Обновление БД по свечам фьючерсов
        futures = pandahouse.read_clickhouse("SELECT figi FROM tinkoff.instruments WHERE instrument_type = 'future'", connection=connection)
        futures = futures['figi'].to_list()
        # information_parser.update_candles_table(figi_list=futures, table='candles', connection=connection)
        print(1)
        df = information_parser.get_history_candles_df('BBG00S76JLH2')


        # print(candles.tail())
        # # Выгрузка всей информации по акциям
        # shares = information_parser.get_shares_df()
        # print(shares.tail())
        # # Выгрузка всей информации по etf
        # etf = information_parser.get_etf_df()
        # print(etf.tail())
        # # Выгрузка всей информации по облигациям
        # bonds = information_parser.get_bonds_df()
        # print(bonds.tail())
        #
        # # Выгрузка всей информации по фьючерсам
        # futures = information_parser.get_futures_df()
        # print(futures.tail())
        #
        # # обновление БД по инструментам
        # information_parser.update_instruments_table(connection=connection)


if __name__ == "__main__":
    main()
