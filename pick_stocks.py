import urllib.request
import urllib.parse
import json
import ssl
import os
import csv
import argparse
import sys
from datetime import date, datetime, timedelta
from typing import List, Optional

def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def resolve_monthly_dir(output_dir: str, code: str) -> str:
    val = os.environ.get("MONTHLY_DIR", "").strip()
    if val:
        base = os.path.expanduser(val)
        if "{code}" in base:
            return base.format(code=code)
        return base
    cand = [
        os.path.join(output_dir, "monthly"),
        os.path.join(output_dir, code, "monthly"),
        os.path.join(output_dir, "monthly", code),
    ]
    for c in cand:
        if os.path.exists(c):
            return c
    return cand[0]

def fmt4s(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{v:.4f}"

def to_float_safe(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def build_10jqka_url_from_text(trade_date: str, query_text: str) -> str:
    base = "https://backtest.10jqka.com.cn/tradebacktest/historypick"
    enc = urllib.parse.quote(query_text, safe="")
    params = f"query={enc}&hold_num=1&trade_date={trade_date}&menv=dma3"
    return f"{base}?{params}"

def fetch_10jqka_picks_text(trade_date: str, query_text: str) -> dict:
    url = build_10jqka_url_from_text(trade_date, query_text)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://backtest.10jqka.com.cn/",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
            txt = resp.read().decode("utf-8", errors="ignore")
        try:
            return json.loads(txt)
        except Exception:
            return {"raw": txt}
    except Exception as e:
        return {"error": str(e), "url": url}

def save_picks_json(output_dir: str, env: str, date_str: str, payload: dict) -> str:
    subdir = os.path.join(output_dir, "10jqka", env)
    ensure_dir(subdir)
    fpath = os.path.join(subdir, f"{date_str}.json")
    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return fpath

def extract_top_picks(picks: dict, k: int = 2) -> List[dict]:
    res: List[dict] = []
    stocks = []
    if isinstance(picks, dict):
        stocks = (picks.get("result", {}).get("stocks") or [])
    for s in stocks[:k]:
        code = s.get("stock_code", "") or ""
        market = s.get("stock_market", "") or ""
        full_code = f"{market}.{code}" if market and code else code
        res.append({
            "code": full_code,
            "name": s.get("stock_name", "") or "",
            "chg": fmt4s(to_float_safe(s.get("change_rate"))),
            "price": fmt4s(to_float_safe(s.get("close_price"))),
            "turnover": fmt4s(to_float_safe(s.get("turnover_rate"))),
        })
    return res

def extract_stock_codes(picks: dict, k: int) -> List[str]:
    if not isinstance(picks, dict):
        return []
    stocks = (picks.get("result", {}).get("stocks") or [])
    codes: List[str] = []
    for s in stocks[: max(0, k)]:
        code = (s or {}).get("stock_code", "") or ""
        code = str(code).strip()
        if code:
            codes.append(code)
    return codes

def resolve_trade_dates(trade_date_str: str) -> List[str]:
    trade_date_str = (trade_date_str or "").strip()
    try:
        base_date = datetime.strptime(trade_date_str, "%Y-%m-%d").date()
    except Exception:
        base_date = datetime.now().date()

    try:
        import exchange_calendars as xcals

        cal = xcals.get_calendar("XSHG")
        session = (
            base_date
            if cal.is_session(base_date)
            else cal.date_to_session(base_date, direction="previous").date()
        )
        prev_session = cal.previous_session(session).date()
        return [session.strftime("%Y-%m-%d"), prev_session.strftime("%Y-%m-%d")]
    except Exception:
        session = base_date
        while session.weekday() >= 5:
            session -= timedelta(days=1)
        prev_session = session - timedelta(days=1)
        while prev_session.weekday() >= 5:
            prev_session -= timedelta(days=1)
        return [session.strftime("%Y-%m-%d"), prev_session.strftime("%Y-%m-%d")]

def main():
    today = datetime.now().strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default=os.environ.get("PICKS_TRADE_DATE", today))
    parser.add_argument(
        "--top-k",
        type=int,
        default=int((os.environ.get("PICK_STOCKS_TOP_K", "6") or "6").strip() or "6"),
    )
    parser.add_argument("--print-codes", action="store_true")
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    def log(message: str) -> None:
        stream = sys.stderr if args.print_codes else sys.stdout
        print(message, file=stream)

    trade_dates = resolve_trade_dates(args.trade_date or today)
    picks_date = trade_dates[0]
    log(f"开始执行选股逻辑，日期: {picks_date}（同时包含上一交易日：{trade_dates[1]}）")
    
    req1_text = "上影或下影,0%<15日涨跌<45%,上日换手<20;5日量价齐升;3.5<收盘价<80;6日无跌＞2%且11日无跌>4%;5日波动<30;收盘价>8日线;5日>25日;5日角度线向上;45>换手>12或9.5>换手>1;-3<涨幅<9.99;80日涨<125;2.4＞量比＞0.9;近6日涨停>=0;3.5＜5日涨＜9;除去st;主板;总市值降序"
    # req2_text = "上影或下影,0%<15日涨跌,上日换手<20%;6日量价齐升;3.5<收盘价<80;6日无跌＞2%;5日波动<23%;11日无跌>4%,收盘价>8日线;5日>25日;5日角度线向上;45%>换手>12%或9.5%>换手>1%;-3%<涨幅<10%;近80日涨幅<160%;2.4＞量比＞0.9;近5日涨停等于1;3.5%＜5日涨＜9%;除去st;主板;总市值降序"
    # req3_text = "上影或下影,0%<15日涨跌<45%,上日换手<20;5日量价齐升;3.5<收盘价<80;6日无跌＞2%且11日无跌>4%;5日波动<30;收盘价>8日线;5日>25日;5日角度线向上;45>换手>2;-3<涨幅<9.99;80日涨<125;2.4＞量比＞0.9;近6日涨停>=0;3.5＜5日涨＜9;除去st;主板;总市值降序前"
    
    results_by_date = {}
    merged_codes: List[str] = []
    merged_seen = set()
    for trade_date in trade_dates:
        log(f"Fetching query 1... trade_date={trade_date}")
        result = fetch_10jqka_picks_text(trade_date, req1_text)
        results_by_date[trade_date] = result
        for code in extract_stock_codes(result, args.top_k):
            if code and code not in merged_seen:
                merged_seen.add(code)
                merged_codes.append(code)

    res1 = results_by_date[picks_date]
    # print("Fetching query 2...")
    # res2 = fetch_10jqka_picks_text(picks_date, req2_text)
    # print("Fetching query 3 (放宽)...")
    # res3 = fetch_10jqka_picks_text(picks_date, req3_text)
    
    if args.print_codes:
        print(",".join(merged_codes))
        return

    combined = {
        "default_query_text": req1_text,
        "results_by_date": results_by_date,
    }

    out_dir = os.path.join(os.getcwd(), "data")
    if not args.no_save:
        picks_path = save_picks_json(out_dir, "dma3", picks_date, combined)
        print(f"已保存10jqka选股: {picks_path}")

    top2 = extract_top_picks(res1, 2)
    if top2:
        try:
            # 尝试更新月度CSV
            csv_subdir = resolve_monthly_dir(out_dir, "sh000001")
            month = picks_date[:7]
            csv_path = os.path.join(csv_subdir, f"{month}.csv")
            
            if os.path.exists(csv_path):
                with open(csv_path, "r", encoding="utf-8") as rf:
                    reader = csv.DictReader(rf)
                    rows = list(reader)
                if rows:
                    last_row = rows[-1]
                    # 更新最后一行
                    last_row.update({
                        "pick1_code": top2[0].get("code", ""),
                        "pick1_name": top2[0].get("name", ""),
                        "pick1_chg": top2[0].get("chg", ""),
                        "pick1_price": top2[0].get("price", ""),
                        "pick1_turnover": top2[0].get("turnover", ""),
                        "pick2_code": top2[1].get("code", "") if len(top2) > 1 else "",
                        "pick2_name": top2[1].get("name", "") if len(top2) > 1 else "",
                        "pick2_chg": top2[1].get("chg", "") if len(top2) > 1 else "",
                        "pick2_price": top2[1].get("price", "") if len(top2) > 1 else "",
                        "pick2_turnover": top2[1].get("turnover", "") if len(top2) > 1 else "",
                    })
                    
                    fieldnames = list(reader.fieldnames or [])
                    for k in ["pick1_code","pick1_name","pick1_chg","pick1_price","pick1_turnover",
                              "pick2_code","pick2_name","pick2_chg","pick2_price","pick2_turnover"]:
                        if k not in fieldnames:
                            fieldnames.append(k)
                            
                    with open(csv_path, "w", encoding="utf-8", newline="") as wf:
                        writer = csv.DictWriter(wf, fieldnames=fieldnames)
                        writer.writeheader()
                        for r in rows[:-1]:
                            writer.writerow(r)
                        writer.writerow(last_row)
                    print(f"已将同花顺前2名选股追加到本月CSV行: {csv_path}")
                else:
                    print(f"CSV文件 {csv_path} 为空，无法追加选股信息")
            else:
                print(f"CSV文件不存在: {csv_path}，跳过追加选股信息")
        except Exception as e:
            print(f"追加CSV时出错: {e}")

if __name__ == "__main__":
    main()
