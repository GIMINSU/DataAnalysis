from make_cci_signal import _make_cci_signal

import pandas as pd
import yfinance as yf

def _collect_cci_signal(self, market_stock_dict):
    print("Do function : _post_signal_to_slack function")
    total_list = []
    total_df = pd.DataFrame()
    for symbol in list(market_stock_dict[self.market].keys()):
        df = pd.DataFrame()
        if self.market == "kospi":
            ticker = symbol+".KS"
        elif self.market == "sp500":
            ticker = symbol

        try:
            
            df = yf.download(ticker, start=self.start_date, auto_adjust=True, actions=True, threads=False).reset_index()
            df["Symbol"] = symbol
            
            signal_dict, signal_df = _make_cci_signal(self, df)

            signal_dict["symbol"] = symbol
            signal_dict["name"] = market_stock_dict[self.market][symbol]
            total_list.append(signal_dict)

            total_df = pd.concat([total_df, signal_df])
        except Exception as e:
            print(f"{self.market} _collect_cci_signal Exception", e)
            pass
            
    r_df = pd.DataFrame(total_list)

    buy_stock_list = []
    sell_stock_list = []
    if self.market == "kospi":
        buy_stock_list = r_df[r_df["trade_signal"]=="Buy"].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"], ascending=False)["name"].tolist()
        sell_stock_list = r_df[r_df["trade_signal"]=="Sell"].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"])["name"].tolist()
        r_df[r_df["name"].isin(buy_stock_list)]

    elif self.market == "sp500":
        buy_stock_list = r_df[r_df["trade_signal"]=="Buy"].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"], ascending=False)["symbol"].tolist()
        sell_stock_list = r_df[r_df["trade_signal"]=="Sell"].sort_values(["cci_buy_and_hold_diff_rtn", f"within_{self.target_days}days_expected_rtn"])["symbol"].tolist()
        

    buy_df = total_df[total_df["trade"] == "Buy"].sort_values("Date").reset_index(drop=True)
    sell_df = total_df[total_df["trade"] == "Sell"].sort_values("Date").reset_index(drop=True)
    daily_buy_stock_count_mean = round(buy_df.groupby("Date")["Symbol"].nunique().reset_index(name="buy_count")["buy_count"].mean(), 2)
    daily_sell_stock_count_mean = round(sell_df.groupby("Date")["Symbol"].nunique().reset_index(name="sell_count")["sell_count"].mean(), 2)

    today_buy_stock_count = buy_df[buy_df["Date"]==buy_df["Date"].iloc[-1]]["Symbol"].nunique()
    today_sell_stock_count = sell_df[sell_df["Date"]==sell_df["Date"].iloc[-1]]["Symbol"].nunique()


    return r_df, buy_stock_list, sell_stock_list, daily_buy_stock_count_mean, daily_sell_stock_count_mean, today_buy_stock_count, today_sell_stock_count