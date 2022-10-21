#
import refinitiv.dataplatform.eikon as ek
import pandas as pd
import numpy as np

ek.set_app_key('d7d5a9b12a79451aa2214e51e37ac9476bc8ca99') # Put your app key here!

assets = ['AAPL.O', 'IVV', 'GLD', 'SHY.O', 'GM'] # put your own stocks here!

prices, prc_err = ek.get_data(
    instruments = assets,
    fields = [
        'TR.OPENPRICE(Adjusted=0)',
        'TR.HIGHPRICE(Adjusted=0)',
        'TR.LOWPRICE(Adjusted=0)',
        'TR.CLOSEPRICE(Adjusted=0)',
        'TR.PriceCloseDate'
    ],
    parameters = {
        'SDate': '2017-09-26',
        'EDate': '2022-09-26',
        'Frq': 'D'
    }
)

divs, div_err = ek.get_data(
    instruments = assets,
    fields = ['TR.DivExDate', 'TR.DivUnadjustedGross'],
    parameters = {
        'SDate': '2017-09-27',
        'EDate': '2022-09-27',
        'Frq': 'D'
    }
)

splits, splits_err = ek.get_data(
    instruments = assets,
    fields = ['TR.CAEffectiveDate', 'TR.CAAdjustmentFactor'],
    parameters = {
        "CAEventType": "SSP",
        'SDate': '2017-09-27',
        'EDate': '2022-09-27',
        'Frq': 'D'
    }
)

prices.rename(
    columns = {
        'Open Price':'open',
        'High Price':'high',
        'Low Price':'low',
        'Close Price':'close'
    },
    inplace = True
)
prices['Date'] = pd.to_datetime(prices['Date']).dt.date
divs.rename(
    columns = {
        'Dividend Ex Date':'Date',
        'Gross Dividend Amount':'div_amt'
    },
    inplace = True
)
divs.dropna(inplace=True)
divs['Date'] = pd.to_datetime(divs['Date']).dt.date
splits.rename(
    columns = {
        'Capital Change Effective Date':'Date',
        'Adjustment Factor':'split_rto'
    },
    inplace = True
)
splits.dropna(inplace=True)
splits['Date'] = pd.to_datetime(splits['Date']).dt.date

dataset = pd.merge(prices, divs, how='outer', on=['Date', 'Instrument'])

dataset['div_amt'].fillna(0, inplace=True)

dataset = pd.merge(dataset, splits, how='outer', on=['Date', 'Instrument'])

dataset['split_rto'].fillna(1, inplace=True)

dataset = dataset[:-2]

if dataset.isnull().values.any():
    raise Exception('missing values detected!')

historical_returns = pd.DataFrame(columns=np.unique(dataset.Instrument))
historical_returns['Date'] = np.unique(dataset.Date)[1:]
historical_returns = historical_returns.reindex(
    columns = list(['Date']) + list(
        historical_returns[np.unique(dataset.Instrument)]
    )
)

for instrument in np.unique(dataset.Instrument):

    df = dataset.loc[dataset.Instrument == instrument, ['Date', 'close', 'div_amt']]
    df = df.iloc[1:]
    df['numerator'] = df.close + df.div_amt

    df2 = dataset.loc[dataset.Instrument == instrument, ['Date', 'close', 'split_rto']]
    df2 = df2.iloc[:-1]
    df2['Date'] = list(df.Date)
    df2['denominator'] = df2.close * df2.split_rto

    df_12 = pd.merge(
        df[['Date', 'numerator']],
        df2[['Date', 'denominator']],
        how='inner',
        on='Date'
    )

    historical_returns[instrument] = np.log(df_12.numerator / df_12.denominator)

historical_returns.to_csv('hist_rtns.csv')