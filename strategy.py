import argparse
import os
import datetime as dt
import matplotlib
matplotlib.use("Agg")
import backtrader as bt
import matplotlib.dates as mdates
import pandas as pd
from clickhouse_driver import Client


class PaginatedClickHouseOHLCV(bt.feed.DataBase):
    lines = ("open", "high", "low", "close", "volume", "openinterest")
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addminperiod(1)
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
        self._last_date = None
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
        def _to_date(v):
            if isinstance(v, dt.datetime):
                return v.date()
            if isinstance(v, dt.date):
                return v
            if isinstance(v, str):
                try:
                    return dt.date.fromisoformat(v[:10])
                except Exception:
                    return dt.date.today()
            return dt.date.today()
        end_date = _to_date(self.p.todate)
        if self._last_date is None:
            q = base.replace("{op}", ">=")
            params = {"code": self.p.code, "bound": _to_date(self.p.fromdate), "end": end_date, "limit": self.p.chunk}
        else:
            q = base.replace("{op}", ">")
            params = {"code": self.p.code, "bound": _to_date(self._last_date), "end": end_date, "limit": self.p.chunk}
        rows = self._client.execute(q, params)
        if rows:
            self._buffer = rows
            self._last_date = rows[-1][0]
        else:
            self._done = True
    def _pop_next(self):
        if not self._buffer:
            self._fetch_page()
            if not self._buffer:
                return None
        return self._buffer.pop(0)
    def _load(self):
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
    # 【关键点1】定义一条新线来存储信号
    lines = ('signal',)
    
    def __init__(self):
        self.sma_fast = {d: bt.ind.SMA(d.close, period=self.p.fast) for d in self.datas}
        self.sma_slow = {d: bt.ind.SMA(d.close, period=self.p.slow) for d in self.datas}
        self.crossover = {d: bt.ind.CrossOver(self.sma_fast[d], self.sma_slow[d]) for d in self.datas}
    def next(self):
        # 每一根K线开始，默认信号为0
        self.lines.signal[0] = 0

        for d in self.datas:
            pos = self.getposition(d)
            if not pos.size and self.crossover[d] > 0:
                self.buy(data=d)
                # 【关键点2】如果是第一只股票(data0)，记录买入信号
                if d == self.datas[0]:
                    self.lines.signal[0] = 1
            elif pos.size and self.crossover[d] < 0:
                self.sell(data=d)
                # 【关键点3】如果是第一只股票(data0)，记录卖出信号
                if d == self.datas[0]:
                    self.lines.signal[0] = -1


def run_backtest(args):
    cerebro = bt.Cerebro(preload=False)
    codes = args.codes or [args.code]
    print(codes)
    for code in codes:
        start_dt = dt.datetime.fromisoformat(args.start)
        end_dt = dt.datetime.fromisoformat(args.end)
        data = PaginatedClickHouseOHLCV(
            host=args.ck_host,
            port=args.ck_port,
            user=args.ck_user,
            password=args.ck_password,
            database=args.ck_database,
            table=args.ck_table,
            code=code,
            fromdate=start_dt,
            todate=end_dt,
            chunk=args.chunk,
        )
        cerebro.adddata(data, name=code)
    cerebro.addstrategy(SmaCross, fast=args.fast, slow=args.slow)
    cerebro.broker.setcash(args.cash)
    cerebro.broker.setcommission(commission=args.commission)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="timereturn")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")
    cerebro.addanalyzer(bt.analyzers.SQN, _name="sqn")
    result = cerebro.run(maxcpus=1, runonce=False)
    strat = result[0]
    tr = strat.analyzers.timereturn.get_analysis()
    sr = strat.analyzers.sharpe.get_analysis()
    dd = strat.analyzers.drawdown.get_analysis()
    ta = strat.analyzers.trades.get_analysis()
    sqn = strat.analyzers.sqn.get_analysis()
    final_value = cerebro.broker.getvalue()
    start_value = args.cash
    pnl = final_value - start_value
    figs = cerebro.plot(style="candlestick")
    out_dir = os.path.dirname(args.output)
    base = os.path.splitext(os.path.basename(args.output))[0]
    os.makedirs(out_dir, exist_ok=True)
    for i, figrow in enumerate(figs):
        for j, fig in enumerate(figrow):
            for ax in fig.axes:
                ax.grid(True, alpha=0.12)
                ax.xaxis.set_major_locator(mdates.YearLocator())
                ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[3, 6, 9, 12]))
                ax.tick_params(axis='x', labelrotation=0)
            fig.set_size_inches(16, 6)
            fig.savefig(os.path.join(out_dir, f"{base}_fig{i}_{j}.png"), dpi=150)
    try:
        import matplotlib.pyplot as plt
        s = pd.Series(tr)
        s.index = pd.to_datetime(s.index)
        s.sort_index(inplace=True)
        eq = (1.0 + s).cumprod() * start_value
        plt.figure(figsize=(14, 4))
        plt.plot(s.index, s.values)
        plt.title("Time Returns")
        plt.grid(alpha=0.2)
        plt.savefig(os.path.join(out_dir, f"{base}_returns.png"), dpi=150)
        plt.figure(figsize=(14, 4))
        plt.plot(eq.index, eq.values)
        plt.title("Equity Curve")
        plt.grid(alpha=0.2)
        plt.savefig(os.path.join(out_dir, f"{base}_equity.png"), dpi=150)
        plt.figure(figsize=(6, 4))
        plt.hist(s.values, bins=50, alpha=0.8)
        plt.title("Return Distribution")
        plt.grid(alpha=0.2)
        plt.show()
        plt.savefig(os.path.join(out_dir, f"{base}_ret_hist.png"), dpi=150)
    except Exception:
        print("exception in plot")
        pass
    try:
        with open(os.path.join(out_dir, f"{base}_trades.txt"), "w") as f:
            f.write(str(ta))
        with open(os.path.join(out_dir, f"{base}_drawdown.txt"), "w") as f:
            f.write(str(dd))
        with open(os.path.join(out_dir, f"{base}_sharpe.txt"), "w") as f:
            f.write(str(sr))
        with open(os.path.join(out_dir, f"{base}_summary.txt"), "w") as f:
            f.write(str({"final_value": final_value, "pnl": pnl, "sqn": sqn}))
    except Exception:
        pass


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