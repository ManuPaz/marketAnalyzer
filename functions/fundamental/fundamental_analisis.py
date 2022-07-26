from utils.dataframes.work_dataframes import merge
def fundamental_correlation_with_prizes(fundamental, price, freq, stock):
    fundamental = (fundamental.resample(freq).mean() / 1000000).astype(float)
    fundamental = fundamental.round(2)
    price = price.resample(freq).last()
    data = merge([fundamental, price])
    data["marginEbitda"] = data["netIncome"] / data["ebitda"]
    data["marginRevenue"] = data["netIncome"] / data["totalRevenue"]
    data["solvencia"] = data["netDebt"] / data["totalStockholderEquity"]
    data["ev/ebitda"] = (data["commonStockSharesOutstanding"] * data[stock] + data["netDebt"]) / data["ebitda"]
    return data.corr()
