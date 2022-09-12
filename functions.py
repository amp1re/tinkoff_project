from typing import Optional
from datetime import datetime, timedelta
import logging

import pandahouse
from tinkoff.invest import (
    RequestError, PortfolioResponse,
    PositionsResponse, PortfolioPosition,
    AccessLevel, Operation, CandleInterval,
    SharesResponse, EtfsResponse, FuturesResponse, OperationsResponse, BondsResponse)
from tinkoff.invest.services import Services
from tinkoff.invest.utils import now
from tinkoff.invest.schemas import InstrumentStatus
import pandas as pd
from pandas import DataFrame
from tqdm.auto import tqdm

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


class PorfolioManager:
    def __init__(self, client: Services):
        self.usdrur = None
        self.client = client
        self.accounts = []
        self.comission = 0.0025  # TODO: прописать парсинг коммиссии

    def cast_money(self, v, to_rub=True):
        """
        https://tinkoff.github.io/investAPI/faq_custom_types/
        :param to_rub:
        :param v:
        :return:
        """
        r = v.units + v.nano / 1e9
        if to_rub and hasattr(v, 'currency') and getattr(v, 'currency') == 'usd':
            r *= self.get_usdrur()

        return r

    def get_usdrur(self):
        """
        Получаю курс только если он нужен
        :return:
        """
        if not self.usdrur:
            try:
                u = self.client.market_data.get_last_prices(figi=['USD000UTSTOM'])
                self.usdrur = self.cast_money(u.last_prices[0].price)
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))

        return self.usdrur

    def get_accounts(self):
        """
            Получаю все аккаунты и буду использовать только те
            кот текущий токен может хотябы читать,
            остальные акк пропускаю
            :return:w
            """
        r = self.client.users.get_accounts()
        for acc in r.accounts:
            if acc.access_level != AccessLevel.ACCOUNT_ACCESS_LEVEL_NO_ACCESS:
                self.accounts.append(acc.id)

        return self.accounts

    def portfolio_pose_todict(self, p: PortfolioPosition):
        """
        Преобразую PortfolioPosition в dict
        :param p:
        :return:
        """
        r = {
            'figi': p.figi,
            'quantity': self.cast_money(p.quantity),
            'expected_yield': self.cast_money(p.expected_yield),
            'instrument_type': p.instrument_type,
            'average_buy_price': self.cast_money(p.average_position_price),
            'current_price': self.cast_money(p.current_price),
            'currency': p.average_position_price.currency,
            'current_nkd': self.cast_money(p.current_nkd),
            'sell_sum': (self.cast_money(p.current_price) + self.cast_money(p.current_nkd)) * self.cast_money(
                p.quantity),
            'comission': self.cast_money(p.current_price) * self.cast_money(p.quantity) * self.comission,
        }

        if r['currency'] == 'usd':
            # expected_yield в Quotation а там нет currency
            r['expected_yield'] *= self.get_usdrur()

        return r

    def get_portfolio_df(self, account_id: str) -> Optional[DataFrame]:
        """
        Преобразую PortfolioResponse в pandas.DataFrame
        :param account_id:
        :return:
        """
        r: PortfolioResponse = self.client.operations.get_portfolio(account_id=account_id)
        if len(r.positions) < 1: return None
        df = pd.DataFrame([self.portfolio_pose_todict(p) for p in r.positions])
        return df

    def get_operations_df(self, account_id: str) -> Optional[DataFrame]:
        """
        Преобразую PortfolioResponse в pandas.DataFrame
        :param account_id:
        :return:
        """
        r: OperationsResponse = self.client.operations.get_operations(
            account_id=account_id,
            from_=datetime(2015, 1, 1),
            to=datetime.utcnow()
        )

        if len(r.operations) < 1: return None
        df = pd.DataFrame([self.operation_todict(p, account_id) for p in r.operations])
        return df

    def operation_todict(self, o: Operation, account_id: str):
        """
        Преобразую PortfolioPosition в dict
        :param o:
        :return:
        """
        r = {
            'acc': account_id,
            'date': o.date,
            'type': o.type,
            'otype': o.operation_type,
            'currency': o.currency,
            'instrument_type': o.instrument_type,
            'figi': o.figi,
            'quantity': o.quantity,
            'state': o.state,
            'payment': self.cast_money(o.payment, False),
            'price': self.cast_money(o.price, False),
        }

        return r

    def get_money_df(self, account_id: str) -> Optional[DataFrame]:
        """
        Преобразую PositionsResponse в pandas.DataFrame
        :param account_id:
        :return:
        """
        r: PositionsResponse = self.client.operations.get_positions(account_id=account_id)
        if len(r.money) < 1: return None
        df = pd.DataFrame([self.money_pose_todict(p) for p in r.money])
        return df

    def money_pose_todict(self, p: PortfolioPosition):
        """
        Преобразую PortfolioPosition в dict
        :param p:
        :return:
        """
        r = {
            'currency': p.currency,
            'quantity': self.cast_money(p),
        }

        return r


class InformationParser(PorfolioManager):

    def get_history_candles_df(self, figi, interval=CandleInterval.CANDLE_INTERVAL_1_MIN):
        df = DataFrame([{
            'figi': figi,
            'time': c.time,
            'volume': c.volume,
            'open': self.cast_money(c.open),
            'close': self.cast_money(c.close),
            'high': self.cast_money(c.high),
            'low': self.cast_money(c.low),
        } for c in tqdm(self.client.get_all_candles(
            figi=figi,
            from_=now() - timedelta(days=365),
            interval=interval,
        ))])
        if not df.empty:

            df['time'] = pd.to_datetime(df['time']).dt.tz_localize(None)
            df['map'] = df['figi'].astype('str') + df['time'].astype('str')

            return df

    def get_shares_df(self) -> Optional[DataFrame]:
        """
        Преобразую SharesResponse в pandas.DataFrame
        """
        r: SharesResponse = self.client.instruments.shares(instrument_status=InstrumentStatus(2))
        if len(r.instruments) < 1: return None
        df = pd.DataFrame([self.share_pose_todict(p) for p in r.instruments])
        df['instrument_type'] = 'share'
        df['first_1min_candle_date'] = pd.to_datetime(df['first_1min_candle_date']).dt.tz_localize(None)
        df['first_1day_candle_date'] = pd.to_datetime(df['first_1day_candle_date']).dt.tz_localize(None)
        return df

    def share_pose_todict(self, p):
        """
        Преобразую Shares в dict
        :param p:
        :return:
        """
        r = {
            'figi': p.figi,
            'ticker': p.ticker,
            'class_code': p.class_code,
            'isin': p.isin,
            'lot': p.lot,
            'currency': p.currency,
            'klong': self.cast_money(p.klong),
            'kshort': self.cast_money(p.kshort),
            'dlong': self.cast_money(p.dlong),
            'dshort': self.cast_money(p.dshort),
            'dlong_min': self.cast_money(p.dlong_min),
            'dshort_min': self.cast_money(p.dshort_min),
            'short_enabled_flag': p.short_enabled_flag,
            'name': p.name,
            'exchange': p.exchange,
            'issue_size': p.issue_size,
            'country_of_risk': p.country_of_risk,
            'country_of_risk_name': p.country_of_risk_name,
            'sector': p.sector,
            'issue_size_plan': p.issue_size_plan,
            'nominal': self.cast_money(p.nominal),
            'trading_status': p.trading_status,
            'otc_flag': p.otc_flag,
            'buy_available_flag': p.buy_available_flag,
            'sell_available_flag': p.sell_available_flag,
            'div_yield_flag': p.div_yield_flag,
            'share_type': p.share_type,
            'min_price_increment': self.cast_money(p.min_price_increment),
            'api_trade_available_flag': p.api_trade_available_flag,
            'uid': p.uid,
            'real_exchange': p.real_exchange,
            'position_uid': p.position_uid,
            'for_iis_flag': p.for_iis_flag,
            'first_1min_candle_date': p.first_1min_candle_date,
            'first_1day_candle_date': p.first_1day_candle_date,
        }

        return r

    def get_etf_df(self) -> Optional[DataFrame]:
        """
        Преобразую EtfsResponse в pandas.DataFrame
        """
        r: EtfsResponse = self.client.instruments.etfs(instrument_status=InstrumentStatus(2))
        if len(r.instruments) < 1: return None
        df = pd.DataFrame([self.etf_pose_todict(p) for p in r.instruments])
        df['instrument_type'] = 'etf'
        df['first_1min_candle_date'] = pd.to_datetime(df['first_1min_candle_date']).dt.tz_localize(None)
        df['first_1day_candle_date'] = pd.to_datetime(df['first_1day_candle_date']).dt.tz_localize(None)
        return df

    def etf_pose_todict(self, p):
        """
        Преобразую Etf в dict
        :param p:
        :return:
        """
        r = {
            'figi': p.figi,
            'ticker': p.ticker,
            'class_code': p.class_code,
            'isin': p.isin,
            'lot': p.lot,
            'currency': p.currency,
            'klong': self.cast_money(p.klong),
            'kshort': self.cast_money(p.kshort),
            'dlong': self.cast_money(p.dlong),
            'dshort': self.cast_money(p.dshort),
            'dlong_min': self.cast_money(p.dlong_min),
            'dshort_min': self.cast_money(p.dshort_min),
            'short_enabled_flag': p.short_enabled_flag,
            'name': p.name,
            'exchange': p.exchange,
            'fixed_commission': self.cast_money(p.fixed_commission),
            'focus_type': p.focus_type,
            'num_shares': self.cast_money(p.num_shares),
            'country_of_risk': p.country_of_risk,
            'country_of_risk_name': p.country_of_risk_name,
            'sector': p.sector,
            'rebalancing_freq': p.rebalancing_freq,
            'trading_status': p.trading_status,
            'otc_flag': p.otc_flag,
            'buy_available_flag': p.buy_available_flag,
            'sell_available_flag': p.sell_available_flag,
            'min_price_increment': self.cast_money(p.min_price_increment),
            'api_trade_available_flag': p.api_trade_available_flag,
            'uid': p.uid,
            'real_exchange': p.real_exchange,
            'position_uid': p.position_uid,
            'for_iis_flag': p.for_iis_flag,
            'first_1min_candle_date': p.first_1min_candle_date,
            'first_1day_candle_date': p.first_1day_candle_date,

        }
        return r

    def get_bonds_df(self) -> Optional[DataFrame]:
        """
        Преобразую BondsResponse в pandas.DataFrame
        :param account_id:
        :return:
        """
        r: BondsResponse = self.client.instruments.bonds(instrument_status=InstrumentStatus(2))
        if len(r.instruments) < 1: return None
        df = pd.DataFrame([self.bond_pose_todict(p) for p in r.instruments])
        df['instrument_type'] = 'bond'
        df['first_1min_candle_date'] = pd.to_datetime(df['first_1min_candle_date']).dt.tz_localize(None)
        df['first_1day_candle_date'] = pd.to_datetime(df['first_1day_candle_date']).dt.tz_localize(None)
        return df

    def bond_pose_todict(self, p):
        """
        Преобразую Bond в dict
        :param p:
        :return:
        """
        r = {
            'figi': p.figi,
            'ticker': p.ticker,
            'class_code': p.class_code,
            'isin': p.isin,
            'lot': p.lot,
            'currency': p.currency,
            'klong': self.cast_money(p.klong),
            'kshort': self.cast_money(p.kshort),
            'dlong': self.cast_money(p.dlong),
            'dshort': self.cast_money(p.dshort),
            'dlong_min': self.cast_money(p.dlong_min),
            'dshort_min': self.cast_money(p.dshort_min),
            'short_enabled_flag': p.short_enabled_flag,
            'name': p.name,
            'exchange': p.exchange,
            'coupon_quantity_per_year': p.coupon_quantity_per_year,
            'nominal': self.cast_money(p.nominal),
            'placement_price': self.cast_money(p.placement_price),
            'aci_value': self.cast_money(p.aci_value),
            'country_of_risk': p.country_of_risk,
            'country_of_risk_name': p.country_of_risk_name,
            'sector': p.sector,
            'issue_kind': p.issue_kind,
            'issue_size': p.issue_size,
            'issue_size_plan': p.issue_size_plan,
            'trading_status': p.trading_status,
            'otc_flag': p.otc_flag,
            'buy_available_flag': p.buy_available_flag,
            'sell_available_flag': p.sell_available_flag,
            'floating_coupon_flag': p.floating_coupon_flag,
            'perpetual_flag': p.perpetual_flag,
            'amortization_flag': p.amortization_flag,
            'min_price_increment': self.cast_money(p.min_price_increment),
            'api_trade_available_flag': p.api_trade_available_flag,
            'uid': p.uid,
            'real_exchange': p.real_exchange,
            'position_uid': p.position_uid,
            'for_iis_flag': p.for_iis_flag,
            'first_1min_candle_date': p.first_1min_candle_date,
            'first_1day_candle_date': p.first_1day_candle_date,

        }
        return r

    def get_futures_df(self) -> Optional[DataFrame]:
        """
        Преобразую FuturesResponse в pandas.DataFrame
        :param account_id:
        :return:
        """
        r: FuturesResponse = self.client.instruments.futures(instrument_status=InstrumentStatus(2))
        if len(r.instruments) < 1: return None
        df = pd.DataFrame([self.future_pose_todict(p) for p in r.instruments])
        df['instrument_type'] = 'future'
        df['first_1min_candle_date'] = pd.to_datetime(df['first_1min_candle_date']).dt.tz_localize(None)
        df['first_1day_candle_date'] = pd.to_datetime(df['first_1day_candle_date']).dt.tz_localize(None)
        return df

    def future_pose_todict(self, p):
        """
        Преобразую Future в dict
        :param p:
        :return:
        """
        r = {
            'figi': p.figi,
            'ticker': p.ticker,
            'class_code': p.class_code,
            'lot': p.lot,
            'currency': p.currency,
            'klong': self.cast_money(p.klong),
            'kshort': self.cast_money(p.kshort),
            'dlong': self.cast_money(p.dlong),
            'dshort': self.cast_money(p.dshort),
            'dlong_min': self.cast_money(p.dlong_min),
            'dshort_min': self.cast_money(p.dshort_min),
            'short_enabled_flag': p.short_enabled_flag,
            'name': p.name,
            'exchange': p.exchange,
            'futures_type': p.futures_type,
            'asset_type': p.asset_type,
            'basic_asset': p.basic_asset,
            'basic_asset_size': self.cast_money(p.basic_asset_size),
            'country_of_risk': p.country_of_risk,
            'country_of_risk_name': p.country_of_risk_name,
            'sector': p.sector,
            'trading_status': p.trading_status,
            'otc_flag': p.otc_flag,
            'buy_available_flag': p.buy_available_flag,
            'sell_available_flag': p.sell_available_flag,
            'min_price_increment': self.cast_money(p.min_price_increment),
            'api_trade_available_flag': p.api_trade_available_flag,
            'uid': p.uid,
            'real_exchange': p.real_exchange,
            'position_uid': p.position_uid,
            'basic_asset_position_uid': p.basic_asset_position_uid,
            'for_iis_flag': p.for_iis_flag,
            'first_1min_candle_date': p.first_1min_candle_date,
            'first_1day_candle_date': p.first_1day_candle_date,
        }
        return r

    def total_instruments_df(self):
        shares = self.get_shares_df()
        etfs = self.get_etf_df()
        bonds = self.get_bonds_df()
        futures = self.get_futures_df()
        concat_df = pd.concat([bonds, shares, etfs, futures], axis=0, join='outer',
                              ignore_index=False, keys=None,
                              levels=None, names=None, verify_integrity=False, copy=True)

        return concat_df

    def update_instruments_table(self, connection):
        q = f'''SELECT figi from {connection["database"]}.{connection["table"]}'''
        figi_exists = pandahouse.read_clickhouse(q, connection=connection)['figi'].to_list()
        concat_df = self.total_instruments_df()
        concat_df = concat_df[~concat_df['figi'].isin(figi_exists)]
        pandahouse.to_clickhouse(concat_df, connection['table'], connection=connection, index=False)
        print(f'Добавлено {concat_df.shape[0]} значений')
        print(concat_df)

    def update_candles_table(self, figi_list, table, connection):
        """

        :param figi_list: список figi свечи, которых нужно достать
        :param table: таблица для обновления
        :param connection: connection pandahouse
        :return: обновляет бд
        """
        for figi in tqdm(figi_list):
            try:
                q = f"SELECT map from {connection['database']}.{table} WHERE figi='{figi}'"
                map_exists = pandahouse.read_clickhouse(q, connection=connection)['map'].to_list()
                figi_df = self.get_history_candles_df(figi)
                if figi_df is not None:
                    figi_df = figi_df[~figi_df['map'].isin(map_exists)]
                    pandahouse.to_clickhouse(figi_df, table, connection=connection, index=False)
                    print(f'У {figi} добавлено {figi_df.shape[0]} свечей')
                else:
                    print(f'По {figi} свечей нет')
                    continue
            except RequestError as err:
                tracking_id = err.metadata.tracking_id if err.metadata else ""
                logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))
