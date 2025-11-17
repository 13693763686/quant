import argparse
import os
import datetime as dt
import matplotlib
matplotlib.use("Agg")
import backtrader as bt
from clickhouse_driver import Client


class PaginatedClickHouseOHLCV(bt.feed.DataBase):
    params = dict(
        host="127.0.0.1",
        port=9000,
        user="default",
        password="",
        database="market",
        table="cn_stock_daily",
        code=None,
        fromdate="2018-01-01",
        todate=dt.date.today().isoformat(),
        chunk=10000,
        timeframe=bt.TimeFrame.Days,
        compression=1,
    )
    def start(self):
        super().start()
        self._client = Client(
            host=self.p.host,
            port=self.p.port,
            user=self.p.user,
            password=self.p.password,
            database=self.p.database,
            settings={"use_numpy": False},
        )
        self._buffer = []
        self._done = False
        self._last = None
    def stop(self):
        try:
            if hasattr(self, "_client") and self._client is not None:
                self._client.disconnect()
        except Exception:
            pass
    def _parse_dt(self, d):
        if isinstance(d, dt.datetime):
            return d
        if isinstance(d, dt.date):
            return dt.datetime.combine(d, dt.time())
        if isinstance(d, str):
            try:
                return dt.datetime.fromisoformat(d)
            except Exception:
                return dt.datetime.strptime(d[:10], "%Y-%m-%d")
        return dt.datetime.now()
    def _fetch_page(self):
        if self._done:
            return
        base = (
            f"SELECT date, open, high, low, close, volume FROM {self.p.database}.{self.p.table} "
            f"WHERE code=%(code)s AND date {{op}} %(bound)s AND date <= %(end)s ORDER BY date LIMIT %(limit)s"
        )
        if self._last is None:
            q = base.replace("{op}", ">=")
            params = {"code": self.p.code, "bound": self.p.fromdate, "end": self.p.todate, "limit": self.p.chunk}
        else:
            q = base.replace("{op}", ">")
            params = {"code": self.p.code, "bound": self._last, "end": self.p.todate, "limit": self.p.chunk}
        rows = self._client.execute(q, params)
        if rows:
            self._buffer = rows
            self._last = rows[-1][0]
        else:
            self._done = True
    def _pop_next(self):
        if not self._buffer:
            self._fetch_page()
            if not self._buffer:
                return None
        return self._buffer.pop(0)
    def load(self):
        row = self._pop_next()
        if row is None:
            return False
        d, o, h, l, c, v = row
        dtobj = self._parse_dt(d)
        self.lines.datetime[0] = bt.date2num(dtobj)
        self.lines.open[0] = float(o)
        self.lines.high[0] = float(h)
        self.lines.low[0] = float(l)
        self.lines.close[0] = float(c)
        self.lines.volume[0] = float(v)
        self.lines.openinterest[0] = 0.0
        return True


class SmaCross(bt.Strategy):
    params = dict(fast=10, slow=30)
    def __init__(self):
        self.sma_fast = {d: bt.ind.SMA(d.close, period=self.p.fast) for d in self.datas}
        self.sma_slow = {d: bt.ind.SMA(d.close, period=self.p.slow) for d in self.datas}
        self.crossover = {d: bt.ind.CrossOver(self.sma_fast[d], self.sma_slow[d]) for d in self.datas}
    def next(self):
        for d in self.datas:
            pos = self.getposition(d)
            if not pos.size and self.crossover[d] > 0:
                self.buy(data=d)
            elif pos.size and self.crossover[d] < 0:
                self.sell(data=d)


def run_backtest_pandas_duplicate_removed(args):
    cerebro = bt.Cerebro()
    codes = args.codes or [args.code]
    for code in codes:
        data = PaginatedClickHouseOHLCV(
            host=args.ck_host,
            port=args.ck_port,
            user=args.ck_user,
            password=args.ck_password,
            database=args.ck_database,
            table=args.ck_table,
            code=code,
            fromdate=args.start,
            todate=args.end,
            chunk=args.chunk,
        )
        cerebro.adddata(data, name=code)
    cerebro.addstrategy(SmaCross, fast=args.fast, slow=args.slow)
    cerebro.broker.setcash(args.cash)
    cerebro.broker.setcommission(commission=args.commission)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="timereturn")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    result = cerebro.run(maxcpus=1)
    strat = result[0]
    tr = strat.analyzers.timereturn.get_analysis()
    sr = strat.analyzers.sharpe.get_analysis()
    dd = strat.analyzers.drawdown.get_analysis()
    final_value = cerebro.broker.getvalue()
    start_value = args.cash
    pnl = final_value - start_value
    print({"final_value": final_value, "pnl": pnl, "returns": tr, "sharpe": sr, "drawdown": dd})
    figs = cerebro.plot(style="candlestick")
    fig = figs[0][0]
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    fig.savefig(args.output, dpi=150)


def parse_args():
    p = argparse.ArgumentParser(description="Backtrader 双均线策略，数据源 ClickHouse")
    p.add_argument("--ck-host", default="127.0.0.1")
    p.add_argument("--ck-port", type=int, default=9000)
    p.add_argument("--ck-user", default="default")
    p.add_argument("--ck-password", default="")
    p.add_argument("--ck-database", default="market")
    p.add_argument("--ck-table", default="cn_stock_daily")
    p.add_argument("--code")
    p.add_argument("--codes", nargs="*", help="可选：多股票代码，如 sh.600000 sz.000001")
    p.add_argument("--start", default="2018-01-01")
    p.add_argument("--end", default=dt.date.today().isoformat())
    p.add_argument("--fast", type=int, default=10)
    p.add_argument("--slow", type=int, default=30)
    p.add_argument("--cash", type=float, default=100000.0)
    p.add_argument("--commission", type=float, default=0.001)
    p.add_argument("--output", default="./output/backtest.png")
    p.add_argument("--chunk", type=int, default=10000)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_backtest(args)
# /Users/xiaolongzhang/go/src/trae/quant/strategy.py




def load_df_from_clickhouse_duplicate_removed(host, port, user, password, database, table, code, start, end):
    client = Client(host=host, port=port, user=user, password=password, database=database, settings={"use_numpy": False})
    rows = client.execute(
        f"SELECT date, open, high, low, close, volume FROM {database}.{table} WHERE code=%(code)s AND date >= %(start)s AND date <= %(end)s ORDER BY date",
        {"code": code, "start": start, "end": end},
    )
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df.sort_index(inplace=True)
    return df

def run_backtest(args):
    df = load_df_from_clickhouse(
        host=args.ck_host,
        port=args.ck_port,
        user=args.ck_user,
        password=args.ck_password,
        database=args.ck_database,
        table=args.ck_table,
        code=args.code,
        start=args.start,
        end=args.end,
    )
    cerebro = bt.Cerebro()
    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data, name=args.code)
    cerebro.addstrategy(SmaCross, fast=args.fast, slow=args.slow)
    cerebro.broker.setcash(args.cash)
    cerebro.broker.setcommission(commission=args.commission)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="timereturn")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    result = cerebro.run(maxcpus=1)
    strat = result[0]
    tr = strat.analyzers.timereturn.get_analysis()
    sr = strat.analyzers.sharpe.get_analysis()
    dd = strat.analyzers.drawdown.get_analysis()
    final_value = cerebro.broker.getvalue()
    start_value = args.cash
    pnl = final_value - start_value
    print({"final_value": final_value, "pnl": pnl, "returns": tr, "sharpe": sr, "drawdown": dd})
    figs = cerebro.plot(style="candlestick")
    fig = figs[0][0]
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    fig.savefig(args.output, dpi=150)

def parse_args():
    p = argparse.ArgumentParser(description="Backtrader 双均线策略，数据源 ClickHouse")
    p.add_argument("--ck-host", default="127.0.0.1")
    p.add_argument("--ck-port", type=int, default=9000)
    p.add_argument("--ck-user", default="default")
    p.add_argument("--ck-password", default="")
    p.add_argument("--ck-database", default="market")
    p.add_argument("--ck-table", default="cn_stock_daily")
    p.add_argument("--code", required=True)
    p.add_argument("--start", default="2018-01-01")
    p.add_argument("--end", default="2025-11-17")
    p.add_argument("--fast", type=int, default=10)
    p.add_argument("--slow", type=int, default=30)
    p.add_argument("--cash", type=float, default=100000.0)
    p.add_argument("--commission", type=float, default=0.001)
    p.add_argument("--output", default="./output/backtest.png")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run_backtest(args)