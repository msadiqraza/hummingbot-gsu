import asyncio
import os
import time
from decimal import ROUND_UP
from typing import Dict

import requests
from pydantic import Field
from web3 import Web3

from hummingbot.client.config.config_data_types import BaseClientModel, ClientFieldData
from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import Decimal, ScriptStrategyBase, logging


class GSUStrategyConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))

    # Optional
    connector_chain_network: str = Field(
        "balancer_ethereum_mainnet", client_data=ClientFieldData(prompt_on_new=False, prompt=lambda mi: "DEX connector")
    )
    network: str = Field("sepolia", client_data=ClientFieldData(prompt_on_new=False, prompt=lambda mi: "Network sepolia or mainnet"))
    trading_pair: str = Field("GXX1-UXXX1", client_data=ClientFieldData(prompt_on_new=False, prompt=lambda mi: "Trading pair"))
    external_rate_api: str = Field(
        "https://gsurates.com/rates", client_data=ClientFieldData(prompt_on_new=False, prompt=lambda mi: "External rate API")
    )
    balancer_vault_address: str = Field(
        "0xBA12222222228d8Ba445958a75a0704d566BF2C8", client_data=ClientFieldData(prompt_on_new=False, prompt=lambda mi: "Balancer Vault Address")
    )
    external_rate_api_pair: str = Field("gsuusdt", client_data=ClientFieldData(prompt_on_new=False, prompt=lambda mi: "External rate API pair"))

    # Required
    slippage_buffer: Decimal = Field(0.5, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Slippage (in percent)"))
    profit_threshold_per_token: Decimal = Field(
        0.001, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Minimum rate difference for arbitrage (in units)")
    )
    minimum_profit: Decimal = Field(10, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Minimum profit (units)"))
    maximum_order_amount: Decimal = Field(1000, client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Maximum order amount (in units)"))
    rpc_url: str = Field("http://localhost:8545", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Infura/Alchemy RPC URL"))


class AmmPriceExample(ScriptStrategyBase):
    """
    This example shows how to call the /amm/price Gateway endpoint to fetch price for a swap
    """

    w3 = None
    rounding = ROUND_UP
    on_going_task = False
    decimals_format = Decimal("1.000000")
    swap_event_abi = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "bytes32", "name": "poolId", "type": "bytes32"},
                {"indexed": True, "internalType": "contract IERC20", "name": "tokenIn", "type": "address"},
                {"indexed": True, "internalType": "contract IERC20", "name": "tokenOut", "type": "address"},
                {"indexed": False, "internalType": "uint256", "name": "amountIn", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "amountOut", "type": "uint256"},
            ],
            "name": "Swap",
            "type": "event",
        }
    ]

    @classmethod
    def init_markets(cls, config: GSUStrategyConfig):
        cls.markets = {config.connector_chain_network: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: GSUStrategyConfig):
        super().__init__(connectors)

        self.config = config
        self.network = config.network
        self.base, self.quote = config.trading_pair.split("-")
        self.connector, self.chain, _network = config.connector_chain_network.split("_")
        self.external_rate_api = f"{config.external_rate_api}/{config.external_rate_api_pair}"
        self.pending_transactions = {}  # Map to store pending transactions

        self.setup_logging()
        self.init_web3()
        self.wallet_address = self.get_wallet()

    def on_tick(self):
        # only execute once
        if not self.on_going_task:
            self.on_going_task = True
            # wrap async task in safe_ensure_future
            safe_ensure_future(self.async_task())

    def setup_logging(self):
        self.logger().setLevel(logging.DEBUG)

    def init_web3(self):
        self.w3 = Web3(Web3.HTTPProvider(self.config.rpc_url))
        self.vault_contract_address = self.w3.to_checksum_address(self.config.balancer_vault_address)
        self.contract = self.w3.eth.contract(self.vault_contract_address, abi=self.swap_event_abi)

    def complete_async_task(self):
        if self.on_going_task:
            self.on_going_task = False
            self.logger().info("async_task completed")
            return

    # async task since we are using Gateway
    async def async_task(self):
        try:
            self.logger().info("async_task started")

            # Check for pending transactions
            if len(self.pending_transactions) > 0:
                self.logger().info(f"Wait for Pending transactions to be confirmed: {list(self.pending_transactions.keys())}")
                self.complete_async_task()

            # 1st Step `Has enough gas for tx`
            has_gas = await self.has_gas_for_tx()
            if has_gas is False:
                self.complete_async_task()

            # 2nd Step `check for potential profit`
            potential_profit, base_diff, quote_diff = await self.checkPotentialProfit()
            if potential_profit is False:
                self.complete_async_task()

            # 3rd Step `find action to take`
            base, quote, rate_spread = self.find_action(base_diff, quote_diff)

            # 4th Step `calculate amount to sell`
            amount, price = await self.calculate_sell_amount(base, quote, rate_spread)
            if amount == 0:
                self.complete_async_task()

            # 5th Step `validate token balance`
            has_balance = await self.has_balance(base, amount)
            if has_balance is False:
                self.complete_async_task()

            # 6th Step `check for pending txs`
            pending_txs = await self.get_pending_txs()
            if pending_txs is True:
                self.logger().error("Handle pending txs")
                self.complete_async_task()

            # 7th Step `execute trade`
            trade_data = await self.execute_aam_trade(base, quote, amount, price, TradeType.SELL)
            # trade_data = await self.execute_aam_trade("UXXX1", "GXX1", Decimal(10), Decimal("1.0317486"), TradeType.SELL)
            # self.logger().debug(f"Trade data: {trade_data}")

            await asyncio.sleep(5)

            asyncio.create_task(self.poll_transaction(self.chain, self.network, trade_data["txHash"]))

            self.complete_async_task()

        except Exception as e:
            self.logger().error(f"Error in async_task: {str(e)}")
            # Reset the task flag so it can be retried if needed
            self.on_going_task = False

    def get_wallet(self):
        gateway_connections_conf = GatewayConnectionSetting.load()
        if len(gateway_connections_conf) > 1:
            self.notify("No existing wallet.\n")
            return None

        wallet = [w for w in gateway_connections_conf if w["chain"] == self.chain and w["connector"] == self.connector]
        wallet_address = wallet[0]["wallet_address"]
        return wallet_address

    async def checkPotentialProfit(self) -> tuple[bool, Decimal, Decimal]:
        try:
            result = await self.fetch_rates(self.base, self.quote, TradeType.SELL)
            external_rate_base, external_rate_quote = result["external_rate_base"], result["external_rate_quote"]
            pool_rate_base, pool_rate_quote = result["pool_rate_base"], result["pool_rate_quote"]

            # Calculate differences for both base and quote
            base_diff = pool_rate_base - external_rate_base
            quote_diff = pool_rate_quote - external_rate_quote
            max_diff = max(base_diff, quote_diff)

            self.logger().info(
                f"""
            Rate Analysis:
            External Base Rate: {external_rate_base}
            Pool Base Rate: {pool_rate_base}
            Base Difference: {base_diff}
            External Quote Rate: {external_rate_quote}
            Pool Quote Rate: {pool_rate_quote}
            Quote Difference: {quote_diff}
            Max Difference: {max_diff}
            """
            )

            if max_diff > self.config.profit_threshold_per_token:
                return True, base_diff, quote_diff

            self.logger().info("No profitable opportunities found above thresholds")
            return False, Decimal(0), Decimal(0)

        except Exception as e:
            self.logger().error(f"Error in profit calculation: {str(e)}")
            return False, Decimal(0), Decimal(0)

    def find_action(self, base_diff, quote_diff) -> tuple[str, str, Decimal]:
        try:
            if base_diff > quote_diff:
                trade_direction = f"SELL {self.base} for {self.quote}"
                base = self.base
                quote = self.quote
                rate_spread = base_diff
            else:
                trade_direction = f"SELL {self.quote} for {self.base}"
                base = self.quote
                quote = self.base
                rate_spread = quote_diff

            self.logger().info(f"Action: {trade_direction}")

            return (base, quote, rate_spread)

        except Exception as e:
            self.logger().error(f"Error in finding action: {str(e)}")
            return False, None, None

    async def calculate_sell_amount(self, base, quote, rate_spread) -> tuple[Decimal, Decimal]:
        gas_in_tokens = await self.tx_cost_in_tokens(quote)
        minimum_order_amount = self.calculate_minimum_order_amount(rate_spread, gas_in_tokens)

        if minimum_order_amount > self.config.maximum_order_amount:
            self.logger().info(
                f"Minimum order amount {minimum_order_amount} is higher than maximum order amount {self.config.maximum_order_amount}. Skipping."
            )
            return Decimal(0), Decimal(0)

        self.logger().info(f"Minimum order amount {minimum_order_amount}")

        amount, price = await self.find_optimal_amount(base, quote, minimum_order_amount, gas_in_tokens)
        return amount, price

    async def tx_cost_in_tokens(self, token: str) -> Decimal:
        # average_gas_cost = self.get_average_gas_cost_of_last_3_swaps()
        average_gas_cost = await self.estimate_gas_using_gateway()
        average_gas_cost_usd = self.eth_to_usd(average_gas_cost)
        gas_in_tokens = await self.usdt_to_token(token, average_gas_cost_usd)
        self.logger().info(f"Average tx cost in ethers {average_gas_cost}, in USD {average_gas_cost_usd}, in {token} {gas_in_tokens}")
        return gas_in_tokens

    def calculate_minimum_order_amount(self, rate_spread: Decimal, gas_in_tokens: Decimal) -> Decimal:
        # Total required profit (gas + minimum profit)
        total_required = gas_in_tokens + self.config.minimum_profit
        self.logger().info(f"Total required token to be sold to have a minimum profit {total_required}")

        # Minimum amount needed = total required profit / quote difference + slippage buffer
        min_amount = (total_required / rate_spread) * (1 + self.config.slippage_buffer)

        return int(min_amount)

    async def find_optimal_amount(self, base: str, quote: str, minimum_order_amount: Decimal, gas_in_tokens: Decimal) -> tuple[Decimal, Decimal]:
        """
        Find optimal trade amount by testing 3 different sizes in parallel
        """
        best_profit = Decimal("0")
        best_amount = Decimal("0")
        price = Decimal("0")

        external_rate = await self.fetch_gsu_rate()
        if base == "UXXX1" or base == "USDT":
            external_rate = external_rate.get("quote")
        else:
            external_rate = external_rate.get("base")

        # Calculate 3 test amounts spread between min and max
        spread = int((self.config.maximum_order_amount - minimum_order_amount) / 2)
        test_amounts = [minimum_order_amount, minimum_order_amount + spread, self.config.maximum_order_amount]

        # Create tasks for parallel gateway requests
        tasks = []
        for amount in test_amounts:
            task = GatewayHttpClient.get_instance().get_price(self.chain, self.network, self.connector, base, quote, amount, TradeType.SELL)
            tasks.append(task)

        # Execute requests in parallel
        self.logger().info(f"Getting prices for {base} to {quote} with amounts {test_amounts}")
        pool_results = await asyncio.gather(*tasks)

        # Analyze results for each amount
        for i, pool_price_data in enumerate(pool_results):
            current_amount = test_amounts[i]

            expected_pool_amount = Decimal(pool_price_data["expectedAmount"])
            price = Decimal(pool_price_data["price"])

            external_expected_amount = current_amount * external_rate

            amount_difference = expected_pool_amount - external_expected_amount

            if amount_difference < 0:
                continue

            potential_profit = amount_difference * external_rate
            net_profit = potential_profit - gas_in_tokens

            self.logger().info(
                f"""
                Test Amount: {current_amount}
                Pool Expected: {expected_pool_amount}
                External Expected: {external_expected_amount}
                Pool Price: {price}
                External Rate: {external_rate}
                Potential Profit: {potential_profit}
                Gas Cost: {gas_in_tokens}
                Net Profit: {net_profit}
            """
            )

            if net_profit > best_profit:
                best_profit = net_profit
                best_amount = current_amount

        if best_profit >= self.config.minimum_profit:
            self.logger().info(f"Best amount found: {best_amount} with profit: {best_profit}")
            return best_amount, price

        self.logger().info("No profitable amount found")
        return Decimal("0"), Decimal("0")

    # ---------------------------- Gateway ----------------------------

    async def get_balance(self, address):
        self.logger().debug(f"POST /network/balance [ address: {address}, tokens: ETH, {self.base, self.quote} ]")
        balanceData = await GatewayHttpClient.get_instance().get_balances(self.chain, self.network, address, ["ETH", self.base, self.quote])
        self.logger().info(f"Balances for {address}: {balanceData['balances']}")
        return {
            "ETH": Decimal(balanceData["balances"]["ETH"]),
            self.base: Decimal(balanceData["balances"][self.base]),
            self.quote: Decimal(balanceData["balances"][self.quote]),
        }

    async def get_pending_txs(self):
        return False

    async def execute_aam_trade(self, base: str, quote: str, amount: Decimal, price: Decimal, side: TradeType):
        self.logger().debug(
            f"POST /amm/trade [ connector: {self.connector}, base: {base}, quote: {quote}, amount: {amount}, price: {price} side: {side} ]"
        )
        if side == TradeType.BUY:
            limit_price = Decimal(price * (1 + self.config.slippage_buffer))
        else:
            limit_price = Decimal(price * (1 - self.config.slippage_buffer))

        trade_data = await GatewayHttpClient.get_instance().amm_trade(
            self.chain, self.network, self.connector, self.wallet_address, base, quote, side, amount, limit_price
        )
        self.logger().info(
            f"""
            Trade Executed:
            Base: {base}
            Quote: {quote}
            Amount: {amount}
            Price: {price}
            Limit Price: {limit_price}
            Side: {side}
            Tx hash: {trade_data["txHash"]}
            """
        )
        # Add transaction to pending map
        self.add_pending_transaction(trade_data["txHash"], base, quote, amount, price, side)
        return trade_data

    # continuously poll for transaction until confirmed
    async def poll_transaction(self, chain, network, txHash):
        pending: bool = True
        while pending is True:
            self.logger().debug(f"POST /network/poll [ txHash: {txHash} ]")
            pollData = await GatewayHttpClient.get_instance().get_transaction_status(chain, network, txHash)
            transaction_status = pollData.get("txStatus")
            if transaction_status == 1:
                self.logger().info(f"Trade with transaction hash {txHash} has been executed successfully.")
                self.remove_pending_transaction(txHash)
                pending = False
            elif transaction_status in [-1, 0, 2]:
                self.logger().info(f"Trade is pending confirmation, Transaction hash: {txHash}")
                await asyncio.sleep(5)
            else:
                self.logger().info(f"Unknown txStatus: {transaction_status}")
                self.logger().info(f"{pollData}")
                self.remove_pending_transaction(txHash)
                pending = False

    async def estimate_gas_using_gateway(self):
        return Decimal((await GatewayHttpClient.get_instance().amm_estimate_gas(self.chain, self.network, self.connector))["gasCost"])

    # ---------------------------- Tx Management ----------------------------

    def add_pending_transaction(self, tx_hash: str, base: str, quote: str, amount: Decimal, price: Decimal, side: TradeType):
        """Add a transaction to pending transactions map"""
        self.pending_transactions[tx_hash] = {"base": base, "quote": quote, "amount": amount, "price": price, "side": side, "timestamp": time.time()}

    def remove_pending_transaction(self, tx_hash: str):
        """Remove a transaction from pending transactions map"""
        if tx_hash in self.pending_transactions:
            del self.pending_transactions[tx_hash]

    def get_pending_transactions(self):
        """Get all pending transactions"""
        return self.pending_transactions.copy()

    def get_pending_transaction(self, tx_hash: str):
        """Get specific pending transaction details"""
        return self.pending_transactions.get(tx_hash)

    # # ---------------------------- Validations ----------------------------

    async def has_gas_for_tx(self) -> bool:
        balance = (await self.get_balance(self.wallet_address))["ETH"]
        average_gas_cost = await self.estimate_gas_using_gateway()

        # Check if user has enough ETH for gas
        if balance < average_gas_cost:
            self.logger().error(f"Insufficient ETH. Required: {average_gas_cost}, Available: {balance}")
            return False

        self.logger().info(f"Balance ETH. Required: {average_gas_cost}, Available: {balance}")
        return True

    async def has_balance(self, token: str, amount: Decimal) -> bool:
        balance = (await self.get_balance(self.wallet_address))[token]
        if balance < amount:
            self.logger().error(f"Insufficient balance. Required: {amount}, Available: {balance}")
            return False

        self.logger().info(f"Balance {token}. Required: {amount}, Available: {balance}")
        return True

    # ---------------------------- Rates helper ----------------------------

    async def fetch_rates(self, base: str, quote: str, trade_type: TradeType, amount=Decimal("1")):
        external_rate, pool_rate_base, pool_rate_quote = await asyncio.gather(
            self.fetch_gsu_rate(),
            self.fetch_balancer_rate(base, quote, trade_type, amount),
            self.fetch_balancer_rate(quote, base, trade_type, amount),
        )

        # Check if any of the rates are None
        if None in (external_rate, pool_rate_base, pool_rate_quote):
            self.logger().error("One or more rate fetches failed")
            return None

        external_rate_base, external_rate_quote = external_rate.get("base"), external_rate.get("quote")

        # Additional None checks for nested values
        if None in (external_rate_base, external_rate_quote):
            self.logger().error("Invalid external rate data structure")
            return None

        self.logger().info(f"External api, {self.config.external_rate_api_pair}, base: {external_rate_base} quote: {external_rate_quote}")
        self.logger().info(f"Balancer sdk, {self.base}-{self.quote}, base: {pool_rate_base['price']} quote: {pool_rate_quote['price']}")

        return {
            "external_rate_base": Decimal(external_rate_base),
            "external_rate_quote": Decimal(external_rate_quote),
            "pool_rate_base": Decimal(pool_rate_base["price"]),
            "pool_rate_quote": Decimal(pool_rate_quote["price"]),
        }

    async def fetch_balancer_rate(self, base: str, quote: str, trade_type: TradeType, amount: Decimal):
        try:
            self.logger().debug(
                f"POST /amm/price [ connector: {self.connector}, base: {base}, quote: {quote}, amount: {amount}, side: {trade_type} ]"
            )
            rate = await GatewayHttpClient.get_instance().get_price(self.chain, self.network, self.connector, base, quote, amount, trade_type)
            self.logger().info(f"{trade_type} {base}-{quote}: Amount: {rate['amount']} Price: {rate['price']}")
            return rate
        except requests.RequestException as e:
            self.logger().error(f"Error fetching GSU rate: {str(e)}")
            return None
        except (ValueError, TypeError, ZeroDivisionError) as e:
            self.logger().error(f"Error processing GSU rate data: {str(e)}")
            return None

    async def fetch_gsu_rate(self):
        """
        Fetch GSU rate from external API.
        Replace `external_rate_api` with the actual endpoint for the GSU rate feed.
        """
        try:
            records = requests.get(url=self.external_rate_api).json()
            self.logger().info(f"Rates API: {records}")
            rate = Decimal(records.get("rate"))
            inverse = Decimal(1 / float(rate))
            rate_rounded = rate.quantize(self.decimals_format, rounding=self.rounding)
            inverse_rounded = inverse.quantize(self.decimals_format, rounding=self.rounding)

            return {"base": rate_rounded, "quote": inverse_rounded}
        except requests.RequestException as e:
            self.logger().error(f"Error fetching GSU rate: {str(e)}")
            return None
        except (ValueError, TypeError, ZeroDivisionError) as e:
            self.logger().error(f"Error processing GSU rate data: {str(e)}")
            return None

    # ---------------------------- Utils ----------------------------

    def eth_to_usd(self, amount: Decimal) -> Decimal:
        usd_conversion_rate = RateOracle.get_instance().get_pair_rate("ETH-USD")
        self.logger().info(f"RateOracle ETH to USD conversion rate: {usd_conversion_rate}")
        return (amount * usd_conversion_rate).quantize(self.decimals_format, rounding=self.rounding)

    def usdt_to_usd(self, amount: Decimal) -> Decimal:
        usd_conversion_rate = RateOracle.get_instance().get_pair_rate("USDT-USD")
        self.logger().info(f"USDT to USD conversion rate: {usd_conversion_rate}")
        return (amount * usd_conversion_rate).quantize(self.decimals_format, rounding=self.rounding)

    async def token_to_usdt(self, token: str, amount: Decimal) -> Decimal:
        external_rate_base = (await self.fetch_gsu_rate()).get("base")
        self.logger().info(f"{token} to USDT conversion rate: {external_rate_base}")

        if token == "UXXX1" or token == "USDT":
            return amount

        out = (amount * external_rate_base).quantize(self.decimals_format, rounding=self.rounding)
        return out

    async def usdt_to_token(self, token: str, amount: Decimal) -> Decimal:
        external_rate_quote = (await self.fetch_gsu_rate()).get("quote")
        self.logger().info(f"{token} to USDT conversion rate: {external_rate_quote}")

        if token == "UXXX1" or token == "USDT":
            return amount

        out = (amount * external_rate_quote).quantize(self.decimals_format, rounding=self.rounding)
        return out

    # ---------------------------- Unused ----------------------------

    def get_average_gas_cost_of_last_3_swaps(self):
        # Fetch the last 3 swap events and calculate the average gas cost for them
        from_block = 7290485
        # self.w3.eth.get_block("latest")["number"] - 5000

        swap_events = self.contract.events.Swap().get_logs(fromBlock=from_block, toBlock="latest")

        # Check if there are at least 3 events
        if len(swap_events) < 3:
            self.logger().error("Could not fetch 3 swap events. Not enough events found.")
            return None

        # Get the last 3 events (most recent 3)
        last_3_swap_events = swap_events[-3:]

        total_gas_cost = 0
        for event in last_3_swap_events:
            tx_hash = event.transactionHash.hex()
            tx = self.w3.eth.get_transaction(tx_hash)
            tx_receipt = self.w3.eth.get_transaction_receipt(tx_hash)

            gas_used = tx_receipt["gasUsed"]
            gas_price = tx["gasPrice"]
            transaction_cost = gas_used * gas_price

            # Convert to ETH
            eth_cost = self.w3.from_wei(transaction_cost, "ether")
            self.logger().debug(f"Swap Event Transaction {tx_hash} cost: {eth_cost} ETH")

            total_gas_cost += eth_cost

        # Calculate the average gas cost for the last 3 swaps
        average_gas_cost = total_gas_cost / 3
        return (average_gas_cost).quantize(Decimal("1.0e-18"), rounding=self.rounding)
