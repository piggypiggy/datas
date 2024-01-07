import os
import pandas as pd
import datetime
import numpy as np
import math as m
import scipy.stats as ss


def call_bsm(S0, K, r, T, sqrt_T, Otype, sig):
    d1 = ((m.log(S0/K)) + (r+ (sig*sig)/2)*T)/(sig*sqrt_T)
    d2 = d1 - sig*sqrt_T
    if (Otype == "C"):
        price = S0*(ss.norm.cdf(d1)) \
        - K*(m.exp(-r*T))*(ss.norm.cdf(d2))
        return (price)
    elif (Otype == "P"):
        price  = -S0*(ss.norm.cdf(-d1))\
        + K*(m.exp(-r*T))*(ss.norm.cdf(-d2))
        return price
    

def call_black76(S0, K, r, T, sqrt_T, Otype, sig):
    d1 = ((m.log(S0/K)) + ((sig*sig)/2)*T)/(sig*sqrt_T)
    d2 = d1 - sig*sqrt_T
    if (Otype == "C"):
        price = S0*m.exp(-r*T)*(ss.norm.cdf(d1)) \
        - K*(m.exp(-r*T))*(ss.norm.cdf(d2))
        return (price)
    elif (Otype == "P"):
        price  = -S0*m.exp(-r*T)*(ss.norm.cdf(-d1))\
        + K*(m.exp(-r*T))*(ss.norm.cdf(-d2))
        return price


def vega(S0, K, r, T, sqrt_T, sig):
    d1 = ((m.log(S0/K)) + (r+ (sig*sig)/2)*T)/(sig*sqrt_T)
    vega = S0*(ss.norm.pdf(d1))*sqrt_T
    return vega

def vega76(S0, K, r, T, sqrt_T, sig):
    d1 = ((m.log(S0/K)) + ((sig*sig)/2)*T)/(sig*sqrt_T)
    vega = S0*(ss.norm.pdf(d1))*sqrt_T
    return vega
    
def implied_volatility(S0, K, T, r, price, Otype):
    e = 1e-3
    x0 = 1
    sqrt_T = m.sqrt(T)

    def newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e):
        k=0
        delta = call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price
        while delta > e:
            k=k+1
            if (k > 30):
                return np.nan
            _vega = vega(S0, K, r, T, sqrt_T, x0)
            if (_vega == 0.0):
                return np.nan
            x0 = (x0 - (call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price)/_vega)
            delta = abs(call_bsm(S0, K, r, T, sqrt_T, Otype, x0) - price)
        return x0
    iv = newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e)   
    return iv


def implied_volatility_black76(S0, K, T, r, price, Otype):
    e = 1e-3
    x0 = 1
    sqrt_T = m.sqrt(T)

    def newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e):
        k=0
        delta = call_black76(S0, K, r, T, sqrt_T, Otype, x0) - price
        while delta > e:
            k=k+1
            if (k > 30):
                return np.nan
            _vega = vega76(S0, K, r, T, sqrt_T, x0)
            if (_vega == 0.0):
                return np.nan
            x0 = (x0 - (call_black76(S0, K, r, T, sqrt_T, Otype, x0) - price)/_vega)
            delta = abs(call_black76(S0, K, r, T, sqrt_T, Otype, x0) - price)
        return x0
    iv = newtons_method(S0, K, T, sqrt_T, r, price, Otype, x0, e)   
    return iv


def calculate_greeks(S0, K, T, r, price, Otype):
    if (np.isnan(S0) or np.isnan(K) or np.isnan(price) or S0==0.0 or price==0.0):
        return [np.nan, np.nan]
    
    if (Otype == 'C' and S0/K > 1.25):
        return [0, 0]

    if (Otype == 'P' and K/S0 > 1.25):
        return [0, 0]
    
    if (T < 0.25/365):
        return [np.nan, np.nan]

    sqrt_T = m.sqrt(T)

    # print(S0, K, T, r, price, Otype)

    # imp_vol
    iv = implied_volatility(S0, K, T, r, price, Otype)
    d1 = ((m.log(S0/K)) + (r + (iv*iv)/2)*T)/(iv*sqrt_T)
    # iv = implied_volatility_black76(S0, K, T, r, price, Otype)
    # d1 = ((m.log(S0/K)) + ((iv*iv)/2)*T)/(iv*sqrt_T)
    

    # delta
    if Otype == 'C':
        delta = ss.norm.cdf(d1)
    else:
        delta = ss.norm.cdf(d1) - 1

    return [round(iv,5), round(delta,4)]



if __name__=="__main__":
    # FUT PRICE
    S0 = 550.4
    # STRIKE
    K = 600
    # TIME TO EXPIRY
    T = 13 / 360
    # INTEREST RATE
    r = 0.02
    # OPTION PRICE
    price = 2.5
    # OPTION TYPE
    Otype = 'C'

    # # FUT PRICE
    # S0 = 19320
    # # STRIKE
    # K = 19400
    # # TIME TO EXPIRY
    # T = 17 / 360
    # # INTEREST RATE
    # r = 0.02
    # # OPTION PRICE
    # price = 305
    # # OPTION TYPE
    # Otype = 'P'

    iv, delta = calculate_greeks(S0, K, T, r, price, Otype)

    print(iv, delta)

    pass

