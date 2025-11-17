import argparse
import datetime as dt
import logging
import socket
import time
from typing import List, Tuple, Optional, Dict


import threading
_BS_ACTIVE = False
_BS_LOCK = threading.Lock()

def bs_login_global(timeout: int = 15, retries: int = 2):
    import baostock as bs
    socket.setdefaulttimeout(timeout)
    last_err = None
    for attempt in range(retries + 1):
        try:
            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock login failed: {lg.error_code} {lg.error_msg}")
            global _BS_ACTIVE
            _BS_ACTIVE = True
            return
        except Exception as e:
            last_err = e
            time.sleep(1.0 + attempt)
    raise RuntimeError(f"baostock global login failed: {last_err}")

def bs_logout_global():
    import baostock as bs
    global _BS_ACTIVE
    if _BS_ACTIVE:
        try:
            alive = bs_session_alive()
            if alive:
                try:
                    bs.logout()
                except Exception:
                    pass
        finally:
            _BS_ACTIVE = False

def bs_session_alive(probe_code: str = "sh.600000", timeout: int = 5) -> bool:
    import baostock as bs
    try:
        socket.setdefaulttimeout(timeout)
        with _BS_LOCK:
            rs = bs.query_stock_basic(code=probe_code)
            return rs.error_code == "0"
    except Exception:
        return False


def _default_fields() -> List[str]:
    return [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "tradestatus",
        "pctChg",
        "isST",
    ]


def get_all_stock_codes() -> List[str]:
    import baostock as bs

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_code} {lg.error_msg}")

    rs = bs.query_all_stock()
    codes: List[str] = []
    while rs.error_code == "0" and rs.next():
        row = rs.get_row_data()
        codes.append(row[0])

    bs.logout()
    return codes


def get_hs300_codes(on_date: Optional[str] = None, timeout: int = 15, retries: int = 2) -> List[str]:
    import baostock as bs
    socket.setdefaulttimeout(timeout)
    d = on_date or dt.date.today().isoformat()
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            with _BS_LOCK:
                rs = bs.query_hs300_stocks(d)
                codes: List[str] = []
                code_idx = None
                try:
                    fields = list(rs.fields)
                    code_idx = fields.index("code") if "code" in fields else 0
                except Exception:
                    code_idx = 0
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    codes.append(row[code_idx])
            return codes
        except Exception as e:
            last_err = e
            time.sleep(1.0 + attempt)
    raise RuntimeError(f"query_hs300_stocks failed: {last_err}")


def get_ipo_date(code: str, timeout: int = 15, retries: int = 2) -> Optional[str]:
    import baostock as bs
    socket.setdefaulttimeout(timeout)

    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            with _BS_LOCK:
                rs = bs.query_stock_basic(code=code)
                ipo: Optional[str] = None
                idx = None
                try:
                    fields = list(rs.fields)
                    idx = fields.index("ipoDate") if "ipoDate" in fields else None
                except Exception:
                    idx = None
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    if idx is not None and idx < len(row):
                        ipo = row[idx] or None
                    elif len(row) >= 3:
                        ipo = row[2] or None
                    break
            return ipo
        except Exception as e:
            last_err = e
            time.sleep(1.0 + attempt)
    raise RuntimeError(f"query_stock_basic failed for {code}: {last_err}")


def fetch_history_for_code(
    code: str,
    start_date: str,
    end_date: str,
    fields: Optional[List[str]] = None,
    timeout: int = 15,
    retries: int = 2,
) -> List[Tuple]:
    import baostock as bs

    fields = fields or _default_fields()
    socket.setdefaulttimeout(timeout)

    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            with _BS_LOCK:
                rs = bs.query_history_k_data_plus(
                    code,
                    ",".join(fields),
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="2",
                )

                data: List[Tuple] = []
                numeric = {"open", "high", "low", "close", "volume", "amount", "turn", "pctChg"}
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    converted: List = []
                    for f, v in zip(fields, row):
                        if f == "date":
                            try:
                                converted.append(dt.date.fromisoformat(v))
                            except Exception:
                                converted.append(None)
                        elif f in {"code", "adjustflag", "tradestatus", "isST"}:
                            converted.append(v)
                        elif f in numeric:
                            try:
                                converted.append(float(v))
                            except Exception:
                                converted.append(0.0)
                        else:
                            converted.append(v)
                    data.append(tuple(converted))
                return data
        except Exception as e:
            last_err = e
            time.sleep(1.0 + attempt)
    raise RuntimeError(f"baostock fetch failed for {code}: {last_err}")


def ensure_clickhouse_table(
    client,
    database: str,
    table: str,
):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table} (
        date Date,
        code String,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        amount Float64,
        adjustflag String,
        turn Float64,
        tradestatus String,
        pctChg Float64,
        isST String
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(date)
    ORDER BY (code, date)
    SETTINGS index_granularity = 8192
    """
    client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    client.execute(ddl)


def insert_rows(
    client,
    database: str,
    table: str,
    rows: List[Tuple],
):
    if not rows:
        return
    partitions = {}
    for r in rows:
        d = r[0]
        if isinstance(d, dt.date):
            key = d.year * 100 + d.month
        else:
            try:
                dd = dt.date.fromisoformat(str(d))
                key = dd.year * 100 + dd.month
            except Exception:
                key = 0
        partitions.setdefault(key, []).append(r)
    for _, chunk in partitions.items():
        client.execute(
            f"INSERT INTO {database}.{table} VALUES",
            chunk,
            types_check=True,
        )


def get_last_date_in_db(client, database: str, table: str, code: str) -> Optional[dt.date]:
    rows = client.execute(
        f"SELECT max(date) FROM {database}.{table} WHERE code=%(code)s",
        {"code": code},
    )
    if not rows:
        return None
    d = rows[0][0]
    return d

def _make_clickhouse_client(params: Dict):
    from clickhouse_driver import Client

    return Client(
        host=params.get("host", "127.0.0.1"),
        port=params.get("port", 9000),
        user=params.get("user", "default"),
        password=params.get("password", ""),
        database=params.get("database", "market"),
        connect_timeout=params.get("connect_timeout", 10),
        send_receive_timeout=params.get("send_receive_timeout", 10),
        settings={"use_numpy": False},
    )


def parallel_pull_and_write(
    start_date: str,
    end_date: str,
    ck_params: Dict,
    table: str = "cn_stock_daily",
    codes: Optional[List[str]] = None,
    workers: int = 8,
    timeout: int = 15,
    per_task_timeout: int = 30,
    retries: int = 2,
    force_update: bool = False,
):
    import concurrent.futures

    logging.info("准备连接 ClickHouse 并确保目标表存在")
    _client_for_ddl = _make_clickhouse_client(ck_params)
    ensure_clickhouse_table(_client_for_ddl, ck_params.get("database", "market"), table)

    if codes is None:
        logging.info("正在获取全部股票代码列表")
        codes = get_all_stock_codes()

    logging.info(f"开始并发拉取: {len(codes)} 支股票，workers={workers}")

    def task(code_: str) -> Tuple[str, int]:
        logging.info(f"开始拉取 {code_}")
        per_thread_client = _make_clickhouse_client(ck_params)
        eff_start = start_date
        if not force_update:
            last = get_last_date_in_db(per_thread_client, ck_params.get("database", "market"), table, code_)
            if last is not None:
                next_day = (last + dt.timedelta(days=1)).isoformat()
                if next_day > end_date:
                    logging.info(f"跳过 {code_}")
                    return code_, 0
                if next_day > eff_start:
                    eff_start = next_day
        data = fetch_history_for_code(code_, eff_start, end_date, _default_fields(), timeout, retries)
        insert_rows(per_thread_client, ck_params.get("database", "market"), table, data)
        logging.info(f"完成拉取 {code_} 行数 {len(data)}")
        return code_, len(data)

    total = 0
    failures = 0
    done = 0
    pbar = None
    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(codes), desc="进度")
    except Exception:
        pbar = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(task, c): c for c in codes}
        for fut, code_ in list(futures.items()):
            try:
                code_ret, count = fut.result(timeout=per_task_timeout)
                if count >= 0:
                    total += count
                else:
                    failures += 1
                done += 1
                logging.info(f"进度 {done}/{len(codes)}")
                if pbar:
                    pbar.update(1)
            except concurrent.futures.TimeoutError:
                logging.error(f"{code_} 超时")
                failures += 1
                done += 1
                logging.info(f"进度 {done}/{len(codes)}")
                if pbar:
                    pbar.update(1)
            except Exception as e:
                logging.error(f"{code_} 拉取/写入失败: {e}")
                failures += 1
                done += 1
                logging.info(f"进度 {done}/{len(codes)}")
                if pbar:
                    pbar.update(1)
    if pbar:
        pbar.close()

    logging.info(f"完成: 写入 {total} 行，失败 {failures} 支")


def parallel_pull_hs300_since_listing(
    ck_params: Dict,
    table: str = "cn_stock_daily",
    on_date: Optional[str] = None,
    workers: int = 8,
    timeout: int = 15,
    per_task_timeout: int = 60,
    retries: int = 2,
    limit: Optional[int] = None,
    force_update: bool = False,
):
    import concurrent.futures

    logging.info("准备连接 ClickHouse 并确保目标表存在")
    _client_for_ddl = _make_clickhouse_client(ck_params)
    ensure_clickhouse_table(_client_for_ddl, ck_params.get("database", "market"), table)

    bs_login_global(timeout, retries)
    logging.info(f"正在获取 HS300 成分股列表 日期 {on_date}")
    codes = get_hs300_codes(on_date, timeout=timeout, retries=retries)
    if limit is not None:
        codes = codes[:limit]
    logging.info(f"HS300 成分股数量 {len(codes)}，workers={workers}")

    def task(code_: str) -> Tuple[str, int]:
        logging.info(f"开始拉取 {code_}")
        start = get_ipo_date(code_, timeout=timeout, retries=retries) or "1990-01-01"
        end = dt.date.today().isoformat()
        per_thread_client = _make_clickhouse_client(ck_params)
        eff_start = start
        if not force_update:
            last = get_last_date_in_db(per_thread_client, ck_params.get("database", "market"), table, code_)
            if last is not None:
                next_day = (last + dt.timedelta(days=1)).isoformat()
                if next_day > end:
                    logging.info(f"跳过 {code_}")
                    return code_, 0
                if next_day > eff_start:
                    eff_start = next_day
        data = fetch_history_for_code(code_, eff_start, end, _default_fields(), timeout, retries)
        insert_rows(per_thread_client, ck_params.get("database", "market"), table, data)
        logging.info(f"完成拉取 {code_} 行数 {len(data)}")
        return code_, len(data)

    total = 0
    failures = 0
    done = 0
    pbar = None
    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(codes), desc="进度")
    except Exception:
        pbar = None
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(task, c): c for c in codes}
            for fut, code_ in list(futures.items()):
                try:
                    _, count = fut.result(timeout=per_task_timeout)
                    if count >= 0:
                        total += count
                    else:
                        failures += 1
                    done += 1
                    logging.info(f"进度 {done}/{len(codes)}")
                    if pbar:
                        pbar.update(1)
                except concurrent.futures.TimeoutError:
                    logging.error(f"{code_} 超时")
                    failures += 1
                    done += 1
                    logging.info(f"进度 {done}/{len(codes)}")
                    if pbar:
                        pbar.update(1)
                except Exception as e:
                    logging.error(f"{code_} 拉取/写入失败: {e}")
                    failures += 1
                    done += 1
                    logging.info(f"进度 {done}/{len(codes)}")
                    if pbar:
                        pbar.update(1)
    finally:
        if pbar:
            pbar.close()
        bs_logout_global()

    logging.info(f"完成: 写入 {total} 行，失败 {failures} 支")


def _parse_args():
    parser = argparse.ArgumentParser(
        description="利用 baostock 并行拉取股票数据并写入 ClickHouse",
    )
    parser.add_argument("--ck-host", default="127.0.0.1")
    parser.add_argument("--ck-port", type=int, default=9000)
    parser.add_argument("--ck-user", default="default")
    parser.add_argument("--ck-password", default="")
    parser.add_argument("--ck-database", default="market")
    parser.add_argument("--ck-table", default="cn_stock_daily")
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--end", default=dt.date.today().isoformat())
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--per-task-timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--hs300", action="store_true")
    parser.add_argument("--index-date", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int)
    parser.add_argument("--force-update", action="store_true")
    parser.add_argument(
        "--codes",
        nargs="*",
        help="可选：指定若干股票代码（如 sh.600000），不指定则抓取全部",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="在无依赖环境下进行流程验证，不实际访问网络或写库",
    )
    return parser.parse_args()


def _dry_run_simulation(args):
    import random

    logging.info("进入 dry-run 模式：不会导入 baostock/clickhouse 依赖")
    codes = args.codes or ["sh.600000", "sz.000001", "sh.601318"]
    workers = args.workers

    def fake_fetch(code):
        n = random.randint(5, 20)
        rows = []
        fields = _default_fields()
        for i in range(n):
            date = (dt.date.fromisoformat(args.start) + dt.timedelta(days=i)).isoformat()
            rows.append(
                (
                    date,
                    code,
                    10.0,
                    10.5,
                    9.8,
                    10.2,
                    100000.0,
                    1000000.0,
                    "2",
                    1.2,
                    "1",
                    0.5,
                    "0",
                )
            )
        return rows

    logging.info(f"模拟并发: {len(codes)} 支股票，workers={workers}")
    import concurrent.futures

    total = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for rows in ex.map(fake_fetch, codes):
            total += len(rows)

    logging.info(f"dry-run 完成：将会插入 {total} 行到 {args.ck_database}.{args.ck_table}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s",
    )
    args = _parse_args()

    if args.dry_run:
        _dry_run_simulation(args)
        return

    ck_params = {
        "host": args.ck_host,
        "port": args.ck_port,
        "user": args.ck_user,
        "password": args.ck_password,
        "database": args.ck_database,
        "connect_timeout": args.timeout,
        "send_receive_timeout": args.timeout,
    }

    if args.hs300:
        parallel_pull_hs300_since_listing(
            ck_params=ck_params,
            table=args.ck_table,
            on_date=args.index_date,
            workers=args.workers,
            timeout=args.timeout,
            per_task_timeout=args.per_task_timeout,
            retries=args.retries,
            limit=args.limit,
            force_update=args.force_update,
        )
    else:
        parallel_pull_and_write(
            start_date=args.start,
            end_date=args.end,
            ck_params=ck_params,
            table=args.ck_table,
            codes=args.codes,
            workers=args.workers,
            timeout=args.timeout,
            per_task_timeout=args.per_task_timeout,
            retries=args.retries,
            force_update=args.force_update,
        )


if __name__ == "__main__":
    main()