from datetime import datetime
import pandas as pd
import numpy as np

def _make_cci_signal(self, df):
    try:
        # 수익률 계산하기
        df["daily_rtn"] = df["Close"].pct_change()  # 퍼센트 변화율
        df["st_rtn"] = (1 + df["daily_rtn"]).cumprod()  # 누적곱 계산함수 return cumulative product over a DataFrame or Series axis.
        gagr = (df.iloc[-1]['st_rtn'] ** (self.financial_engineering_days / len(df.index)) - 1) * 100

        df["TP"] = (df["High"] + df["Low"] + df["Close"]) / 3
        df["sma"] = df["TP"].rolling(self.config_ndays).mean()
        df["mad"] = df["TP"].rolling(self.config_ndays).apply(lambda x: pd.Series(x).mad())
        df["CCI"] = (df["TP"] - df["sma"]) / (0.015 * df["mad"])
        df["pre1day_CCI"] = df["CCI"].shift(1)

        df["trade"] = "Neutral"
        df["trade"] = np.where((df["CCI"] >= -100) & (df["pre1day_CCI"] < -100), "Buy", df["trade"])
        df["trade"] = np.where((df["CCI"] <= 100) & (df["pre1day_CCI"] > 100), "Sell", df["trade"])
        
        df["total_buy_price"] = 0
        df["shares"] = 0
        df["buy_price"] = 0
        df["sell_price"] = 0
        df["target_price"] = 0
        df["days_to_reach_target_price"] = 0
        df["revenue"] = 0
        df["rate"] = 0

        buy_price_list = []
        for i, x in df.iterrows():
            if x["trade"] == "Buy":
                buy_price_list.append(x["Close"])
                df.loc[i, "total_buy_price"] = np.sum(buy_price_list)
                df.loc[i, "shares"] = len(buy_price_list)
                df.loc[i, "buy_price"] = np.mean(buy_price_list)
            elif x["trade"] == "Neutral":
                if i > 0:
                    df.loc[i, "shares"] = df.loc[i-1, "shares"]
                    df.loc[i, "buy_price"] = df.loc[i-1, "buy_price"]
                    df.loc[i, "total_buy_price"] = df.loc[i-1, "total_buy_price"]
            elif x["trade"] == "Sell" and df.loc[i-1, "shares"] > 0:
                first_price = buy_price_list.pop(0)
                df.loc[i, "revenue"] = df.loc[i, "Close"] - first_price
                df.loc[i, "rate"] = df.loc[i, "revenue"] / first_price * 100
                df.loc[i, "sell_price"] = df.loc[i, "Close"]
                df.loc[i, "shares"] = len(buy_price_list)
                if len(buy_price_list) > 0:
                    df.loc[i, "total_buy_price"] = np.sum(buy_price_list)
                    df.loc[i, "buy_price"] = np.mean(buy_price_list)
                else:
                    df.loc[i, "total_buy_price"] = 0
                    df.loc[i, "buy_price"] = 0
        
        buy_rtn_dict = {}
        
        for i in range(1, self.target_days+1):
            df[f"after_{i}day_price"] = df["Close"].shift(i)
            df[f"after_{i}day_rtn"] = (df[f"after_{i}day_price"] - df["Close"]) / df["Close"] *100
            after_nday_rtn = df[df["trade"]=="Buy"][f"after_{i}day_rtn"].mean()
            if i not in buy_rtn_dict:
                buy_rtn_dict[i] = None
            if i in buy_rtn_dict:
                buy_rtn_dict[i] = after_nday_rtn

        within_target_days_expected_rtn = round(np.average(list(buy_rtn_dict.values())), 2)
        max_rtn_day = max(buy_rtn_dict, key=buy_rtn_dict.get)
        max_rtn = round(buy_rtn_dict[max_rtn_day], 2)

        signal_date = datetime.strftime(df.iloc[-1]["Date"], "%Y-%m-%d").replace("\n", "")
        trade_signal = df.iloc[-1]["trade"]
        signal_price = round(df.iloc[-1]["Close"], 2)
        remain_holding_shares = df.iloc[-1]["shares"]
        holding_shares_buy_price = round(df.iloc[-1]["buy_price"], 2)

        cci_rtn = None
        buy_count = len(df[df["trade"] == "Buy"])
        sell_count = len(df[df["trade"] == "Sell"])
        if sell_count >= buy_count:
            cci_rtn = round(df[df["trade"]=="Sell"].reset_index(drop=True).iloc[buy_count-1]["rate"], 2)
        elif sell_count < buy_count:
            cci_rtn = round(df[df["trade"] == "Sell"].iloc[-1]["rate"], 2)

        buy_and_hold_rtn = round(gagr, 2)
        cci_buy_and_hold_diff_rtn = round(cci_rtn - buy_and_hold_rtn, 2)

        signal_dict = {
            "symbol" : None,
            "name" : None,
            "signal_date" : signal_date,
            "trade_signal" : trade_signal,
            "cci_buy_and_hold_diff_rtn" : cci_buy_and_hold_diff_rtn,
            "cci_rtn" : cci_rtn,
            "buy_and_hold_rtn" : buy_and_hold_rtn,
            f"within_{self.target_days}days_expected_rtn" : within_target_days_expected_rtn,
            "max_rtn_day" : max_rtn_day,
            "max_rtn" : max_rtn,
            "signal_price" : signal_price,
            "remain_holding_shares" : remain_holding_shares,
            "holding_shares_buy_price" : holding_shares_buy_price
            }

        return signal_dict, df
        
    except Exception as e:
        print("_make_cci_signal Exception", e)
        pass