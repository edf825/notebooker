# # Current product risk allocations

# +
import pandas as pd

import ahl.returnbreakdown.api as arb
import ahl.returnbreakdown.classification.market as m
import ahl.returnbreakdown.classification.predictor as p
import ahl.returnbreakdown.classification.basics as b
import pm.data.fund_allocations as fa

# + {"tags": ["parameters"]}
# The product to run allocations for
product = 'DMC0'
# The number of years across which to calculate allocations, this is n years before the start of this year
allocation_lookback_years = 3

# -


# +
def remap_sector(sector):
    sector = sector.replace('_', ' ').title()
    if sector in {'Agriculturals', 'Energies', 'Metals (Base)', 'Metals (Precious)', 'Metals'}:
        return 'Commods'
    if sector in {'Bonds', 'Interest Rates'}:
        return 'Bonds & Rates'
    return sector


def new_sector(strategy, sector):
    if sector == 'Other':
        if strategy == 'BOX10':
            sector = 'Bonds'
        elif strategy == 'CETF':
            sector = 'Equities'
        elif strategy == 'XCOMSPD':
            sector = 'Commods'        
    return remap_sector(sector)


def format_risk_allocations(raw_alloc):
    return pd.DataFrame({
        'Risk': raw_alloc.round(3),
        '% Allocation': (arb.normalise_allocations(raw_alloc) * 100).round(2)
    })


# -

last_year = pd.datetime.now().year - 1
start_year = last_year - allocation_lookback_years
start_year = str(start_year)
last_year = str(last_year)

returns = arb.vol_normalised_overlapping_returns(product=product, style=arb.Styles.Sim, window=(start_year, last_year))
returns = returns.loc[start_year: last_year]

grouped = m.sector(returns)
grouped = p.add_level_groupings(grouped)
grouped = b.add_derived_level(grouped, 'grouped_sector', ['strategy', 'sector'], new_sector)

# ## Current Product Allocations
#
#
# Numbers are quoted in two ways:
#
# '% Allocation' - The percentage allocation in each bucket
#
#  Risk          - The risk allocation relative to a risk of 1
#

format_risk_allocations(fa.get_product_strategy_allocations(product, latest=True))

# ## Three Level Allocations

format_risk_allocations(arb.allocations_at_level(grouped, 'three_level_grouping'))

# ## Eight Level Allocations

format_risk_allocations(arb.allocations_at_level(grouped, 'eight_level_grouping'))

# ## Grouped Sector Allocations

format_risk_allocations(arb.allocations_at_level(grouped, 'grouped_sector'))

# ## Sector Allocations

format_risk_allocations(arb.allocations_at_level(grouped, 'sector'))

# ## Grouped Sector/Three Level Cross-Allocations

arb.allocations_at_level(grouped, ['grouped_sector', 'three_level_grouping']).to_frame('Risk').round(3).unstack().fillna('-')

# ## Sector/Three Level Cross-Allocations

arb.allocations_at_level(grouped, ['sector', 'three_level_grouping']).to_frame('Risk').round(3).unstack().fillna('-')
