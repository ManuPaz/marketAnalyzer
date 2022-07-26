# first column is table and second is time column
FUNDAMENTAL_WORD = "fundamental"
PRICES_WORD = "precios"
DAILY_INFO_WORD = "dailyinfo"
DIVIDENDS_WORD = "dividends"
SPLITS_WORD = "splits"
INFO_COMPLETE_US_WORD = "us_infocomplete"
INSTITUTIONAL_HOLDERS_WORD = "institutionalholders"
STOCKS_NEWS_WORD = "stocknews"
EUROPEAN_INDICES = ["IBEX 35", "DAX", "CAC 40", "FTSE 100"]
COUNTRIES_CHANGE_NAME_INVESTING_DATABASE = {"usa": "united states", "uk": "united kingdom"}
DELETE_SINCE_DATE_BY_STOCK = "delete from {} where STOCK=%s and  {}>%s"
DELETE_SINCE_DATE = "delete from {} where  {}>%s"
STOCK_NAMES = "select concat(exchange,'_',code) as stock, name  from stocks"
SECTORS = "select  concat(exchange,'_',stock) as stock, sector   from sectors"
DESCRIPTIONS = "select concat(exchange,'_',stock) as stock, description  from descriptions"
STOCK_NAMES_BY_EXCHANGE = "select concat(exchange,'_',code) as stock, name  from stocks WHERE exchange=%s"
SECTORS_BY_EXCHANGE = "select concat(exchange,'_',stock) as stock, sector   from sectors where exchange=%s"
DESCRIPTIONS_BY_EXCHANGE = "select concat(exchange,'_',stock) as stock, description  from descriptions where exchange=%s"
CREATE_DAILY_INFO_TABLE_BY_EXCHANGE = "create table if not exists {}_dailyinfo(stock varchar(100)," \
                                      "exchange varchar(100),fecha date,per double,marketcap double,revenue double,eps_annual double, revenue_annual double ,eps double,shares_outstanding double," \
                                      "eps_estimation double,eps_estimation_high double,eps_estimation_low double," \
                                      "revenue_estimation double ,revenue_estimation_high double ,revenue_estimation_low double,adjusted_close double, " \
                                      "PRIMARY KEY(fecha,stock,exchange));"
FUNDAMENTAL_YEARLY = "select year(fecha) as year, avg(netincome/1000000)*4 as netincome," \
                     "avg(totalrevenue/1000000)*4 as totalrevenue,avg(grossprofit/1000000)*4 as grossprofit ," \
                     "avg(ebitda/1000000)*4 as ebitda ,sum(freecashflow/1000000) as freecashflow ," \
                     "sum(totalCashFromOperatingActivities/1000000) as  totalCashFromOperatingActivities ," \
                     "sum(capitalExpenditures/1000000)as  capitalExpenditures," \
                     "avg(totalliab/1000000) as totalliab, avg(totalassets/1000000) as totalassets," \
                     "avg(netdebt/1000000) as netdebt,avg(cash/1000000) as cash,avg(cashAndCashEquivalentsChanges/1000000) as cashAndCashEquivalentsChanges," \
                     "avg(treasuryStock) as treasury, avg(longTermDebt/1000000) as long_debt,avg(shortTermDebt/1000000) as short_debt," \
                     "avg(shortLongTermDebtTotal/1000000) as debt,avg(cash/shortLongTermDebtTotal)as cashtodebtratio,  (sum(operatingINcome)/sum(totalrevenue)) as operatingmargin," \
                     "sum(totalcurrentassets)/sum(totalcurrentliabilities) as solvencia , sum(cash)/sum(totalcurrentliabilities) as liquuidez," \
                     "avg(commonstocksharesoutstanding/1000000) from {}_fundamental," \
                     "where stock=%s group by year(fecha) order by  year(fecha) desc;"
FUNDAMENTAL_RAW="select * from {}_fundamental where stock=%s order by fecha asc;"

CREATE_TABLE_NEWS = "create table if not exists {}_stocknews (exchange varchar(100),stock varchar(100), fecha date, link varchar(600), title varchar(600)," \
                    "PRIMARY KEY(exchange,stock,link));"
CREATE_TABLE_INSTITUTIONAL_HOLDERS = "create table if not exists  {}_institutionalholders (exchange varchar(100),stock varchar(100), fecha date, holder varchar(200), valuee double,shares double, outt double," \
                                     "PRIMARY KEY(exchange,stock,fecha,holder));"
CREATE_FUNDAMENTAL_DATA_TABLE_BY_EXCHANGE = 'create table  if not exists {}_fundamental(fecha date,totalAssets double,intangibleAssets double,' \
                                            'earningAssets double,otherCurrentAssets double,totalLiab double,totalStockholderEquity double,' \
                                            'deferredLongTermLiab double,otherCurrentLiab double,commonStock double,retainedEarnings double,' \
                                            'otherLiab double,goodWill double,otherAssets double,cash double,totalCurrentLiabilities double,' \
                                            'netDebt double,shortTermDebt double,shortLongTermDebt double,shortLongTermDebtTotal double,' \
                                            'otherStockholderEquity double,propertyPlantEquipment double,totalCurrentAssets double,' \
                                            'longTermInvestments double,netTangibleAssets double,shortTermInvestments double,' \
                                            'netReceivables double,longTermDebt double,inventory double,accountsPayable double,' \
                                            'totalPermanentEquity double,temporaryEquityRedeemableNoncontrollingInterests double,' \
                                            'accumulatedOtherComprehensiveIncome double,additionalPaidInCapital double,' \
                                            'commonStockTotalEquity double,preferredStockTotalEquity double,retainedEarningsTotalEquity double,' \
                                            'treasuryStock double,accumulatedAmortization double,nonCurrrentAssetsOther double,' \
                                            'deferredLongTermAssetCharges double,nonCurrentAssetsTotal double,capitalLeaseObligations double,' \
                                            'longTermDebtTotal double,nonCurrentLiabilitiesOther double,nonCurrentLiabilitiesTotal double,' \
                                            'negativeGoodwill double,warrants double,preferredStockRedeemable double,capitalSurpluse double,' \
                                            'liabilitiesAndStockholdersEquity double,cashAndShortTermInvestments double,' \
                                            'propertyPlantAndEquipmentGross double,propertyPlantAndEquipmentNet double,' \
                                            'accumulatedDepreciation double,netWorkingCapital double,netInvestedCapital double,commonStockSharesOutstanding double,' \
                                            'investments double,changeToLiabilities double,totalCashflowsFromInvestingActivities double,netBorrowings double,' \
                                            'totalCashFromFinancingActivities double,changeToOperatingActivities double,netIncome double,changeInCash double,' \
                                            'beginPeriodCashFlow double,endPeriodCashFlow double,totalCashFromOperatingActivities double,depreciation double,' \
                                            'otherCashflowsFromInvestingActivities double,dividendsPaid double,changeToInventory double,changeToAccountReceivables double,' \
                                            'salePurchaseOfStock double,otherCashflowsFromFinancingActivities double,changeToNetincome double,capitalExpenditures double,' \
                                            'changeReceivables double,cashFlowsOtherOperating double,exchangeRateChanges double,cashAndCashEquivalentsChanges double,' \
                                            'changeInWorkingCapital double,otherNonCashItems double,freeCashFlow double,researchDevelopment double,' \
                                            'effectOfAccountingCharges double,incomeBeforeTax double,minorityInterest double,sellingGeneralAdministrative double,' \
                                            'sellingAndMarketingExpenses double,grossProfit double,reconciledDepreciation double,ebit double,ebitda double,' \
                                            'depreciationAndAmortization double,nonOperatingIncomeNetOther double,operatingIncome double,otherOperatingExpenses double,' \
                                            'interestExpense double,taxProvision double,interestIncome double,netInterestIncome double,extraordinaryItems double,' \
                                            'nonRecurring double,otherItems double,incomeTaxExpense double,totalRevenue double,totalOperatingExpenses double,costOfRevenue double,' \
                                            'totalOtherIncomeExpenseNet double,discontinuedOperations double,netIncomeFromContinuingOps double,' \
                                            'netIncomeApplicableToCommonShares double,preferredStockAndOtherAdjustments double,stock varchar (100),' \
                                            'exchange varchar (100),PRIMARY KEY(fecha,stock,exchange));'
CREATE_PRICES_DATA_TABLE_BY_EXCHANGE = 'create table if not exists {}_precios(fecha date,Open double,High double,Low double,Close double,Adjusted_close double,Volume double,' \
                                       'stock varchar (100),exchange varchar (100),PRIMARY KEY(fecha,stock,exchange));'

CREATE_SPLITS_TABLE_BY_EXCHANGE = "create table if not exists {}_splits(fecha date,stock varchar(100),exchange varchar(100),Splits double, PRIMARY KEY(stock,exchange,fecha))"
CREATE_DIVIDEND_TABLE_BY_EXCHANGE = "create table if not exists {}_dividends(fecha date,stock varchar(100),exchange varchar(100),dividend double,yield double,type varchar(100), PRIMARY KEY(stock,exchange,fecha))"
CREATE_INFO_COMPLETE_US_TABLE = 'create table if not exists us_infocomplete(profitMargins double,grossMargins double,operatingCashflow double,revenueGrowth double,' \
                                'operatingMargins double,ebitda double,targetLowPrice double,recommendationKey varchar (100),grossProfits double,freeCashflow double,' \
                                'targetMedianPrice double,earningsGrowth double,currentRatio double,returnOnAssets double,' \
                                'numberOfAnalystOpinions double,targetMeanPrice double,debtToEquity double,returnOnEquity double,targetHighPrice double,' \
                                'totalCash double,totalDebt double,totalRevenue double,totalCashPerShare double,revenuePerShare double,' \
                                'quickRatio double,recommendationMean double,annualHoldingsTurnover double,enterpriseToRevenue double,beta3Year double,' \
                                'enterpriseToEbitda double,52WeekChange double,morningStarRiskRating double,forwardEps double,revenueQuarterlyGrowth double,' \
                                'sharesOutstanding double,fundInceptionDate double,annualReportExpenseRatio double,totalAssets double,bookValue double,' \
                                'sharesShort double,sharesPercentSharesOut double,fundFamily double,lastFiscalYearEnd double,heldPercentInstitutions double,' \
                                'netIncomeToCommon double,trailingEps double,lastDividendValue double,SandP52WeekChange double,priceToBook double,' \
                                'heldPercentInsiders double,nextFiscalYearEnd double,yield double,mostRecentQuarter double,shortRatio double,' \
                                'sharesShortPreviousMonthDate double,floatShares double,beta double,enterpriseValue double,priceHint double,' \
                                'threeYearAverageReturn double,lastSplitDate double,lastSplitFactor varchar(100),legalType double,lastDividendDate double,' \
                                'morningStarOverallRating double,earningsQuarterlyGrowth double,priceToSalesTrailing12Months double,dateShortInterest double,' \
                                'pegRatio double,ytdReturn double,forwardPE double,lastCapGain double,shortPercentOfFloat double,sharesShortPriorMonth double,' \
                                'impliedSharesOutstanding double,category double,fiveYearAverageReturn double,previousClose double,regularMarketOpen double,' \
                                'twoHundredDayAverage double,trailingAnnualDividendYield double,payoutRatio double,volume24Hr double,regularMarketDayHigh double,' \
                                'navPrice double,averageDailyVolume10Day double,regularMarketPreviousClose double,fiftyDayAverage double,trailingAnnualDividendRate double, ' \
                                'open double,averageVolume10days double,expireDate double,algorithm double,dividendRate double,' \
                                'exDividendDate double,circulatingSupply double,startDate double,regularMarketDayLow double,trailingPE double,' \
                                'regularMarketVolume double,lastMarket double,maxSupply double,openInterest double,marketCap double,' \
                                'strikePrice double,averageVolume double,dayLow double,ask double,askSize double,volume double,fiftyTwoWeekHigh double,' \
                                'fiveYearAvgDividendYield double,fiftyTwoWeekLow double,bid double,tradeable double,dividendYield double,' \
                                'bidSize double,dayHigh double,regularMarketPrice double,preMarketPrice double,fecha date, ' \
                                'stock varchar(100),exchange varchar(100), PRIMARY KEY(stock,exchange,fecha))'
