import logging
from tinkoff.invest import (
    RequestError)


logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def get_usdrur(client):
    """
    Получаем курс доллара
    :return:
    """

    usdrur = 0
    try:
        u = client.market_data.get_last_prices(figi=['USD000UTSTOM'])
        usdrur = cast_money(client, u.last_prices[0].price)
    except RequestError as err:
        tracking_id = err.metadata.tracking_id if err.metadata else ""
        logger.error("Error tracking_id=%s code=%s", tracking_id, str(err.code))


def cast_money(client, v, to_rub=True):
    """
    Преоборазование в float (если нужно, то конвертация из USD в RUB)
    :param client:
    :param to_rub:
    :param v: Quotation from API
    :return: Price in standart float type
    """

    r = v.units + v.nano / 1e9  # nano - 9 нулей
    if to_rub and hasattr(v, 'currency') and getattr(v, 'currency') == 'usd':
        r *= get_usdrur(client)

    return r
