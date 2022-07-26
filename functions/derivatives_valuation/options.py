
import math
from scipy.stats import norm
import operator

def statistic(future_value,strike,rate,volatility,T,operador):
    return (math.log(future_value/strike)+(operador(rate,volatility**2/2))*T)/(volatility*math.sqrt(T))
def put_prize(strike,underliying,dividend,rate,T,volatility):

    x1=statistic(underliying-dividend,strike,rate,volatility,T,operator.add)
    x2 = statistic(underliying - dividend, strike, rate, volatility, T, operator.sub)
    prob1=norm.cdf(-x1)
    prob2 = norm.cdf(-x2)
    return -(underliying-dividend)*prob1+strike*prob2*math.exp(-rate*T)



