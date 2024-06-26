from typing import Optional


class BaseClient:
    """
    Abstraction of betting client.
    """

    EXCHANGE = None

    def __init__(
        self,
        betting_client=None,
        interactive_login: bool = False,
        username: str = None,
        order_stream: bool = True,
        paper_trade: bool = False,
        simulated_full_match: bool = False,  # TODO trial adding
    ):
        if hasattr(betting_client, "lightweight"):
            assert (
                betting_client.lightweight is False
            ), "flumine requires resources, please set lightweight to False"
        self._username = username
        self.betting_client = betting_client
        self.interactive_login = interactive_login
        self.order_stream = order_stream
        self.paper_trade = paper_trade  # simulated order placement using live data
        self.simulated_full_match = (  # TODO maybe remove maybe
            simulated_full_match  # simulated 100% match on successful place
        )

        self.account_details = None
        self.account_funds = None
        self.commission_paid = 0
        self.trading_controls = []

    def login(self) -> None:
        raise NotImplementedError

    def keep_alive(self) -> None:
        raise NotImplementedError

    def logout(self) -> None:
        raise NotImplementedError

    def update_account_details(self) -> None:
        raise NotImplementedError

    @property
    def username(self) -> str:
        if self.betting_client:
            return self.betting_client.username
        else:
            return self._username

    @property
    def min_bet_size(self) -> Optional[float]:
        raise NotImplementedError

    @property
    def min_bet_payout(self) -> Optional[float]:
        raise NotImplementedError

    @property
    def min_bsp_liability(self) -> Optional[float]:
        raise NotImplementedError

    @property
    def info(self) -> dict:
        return {
            "username": self.username,
            "exchange": self.EXCHANGE.value if self.EXCHANGE else None,
            "betting_client": self.betting_client,
            "trading_controls": self.trading_controls,
            "order_stream": self.order_stream,
            "paper_trade": self.paper_trade,
        }
