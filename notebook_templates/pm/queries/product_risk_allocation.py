# # Current product risk allocations

# +
import pandas as pd

import ahl.returnbreakdown.api as arb
import ahl.returnbreakdown.classification.market as m
import ahl.returnbreakdown.classification.predictor as p
import ahl.returnbreakdown.classification.basics as b

# + {"tags": ["parameters"]}
# The product to run allocations for
product = 'DMC0'
# The number of years across which to calculate allocations, this is n years before the start of this year
allocation_lookback_years = 3


# -

# Set up some corrections for sectors which are in line with the CPM dimension allocation questions

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
        if strat == 'BOX10':
            sector = 'Bonds'
        elif strat == 'CETF':
            sector = 'Equities'
        elif strat == u'XCOMSPD':
            sector = 'Commods'        
    return remap_sector(sector)


# -

last_year = pd.datetime.now().year - 1
start_year = last_year - allocation_lookback_years

returns = arb.vol_normalised_overlapping_returns(product=product, style=arb.Styles.Sim, window=(str(start_year), str(last_year)))

grouped = m.sector(returns)
grouped = p.add_level_groupings(grouped)
grouped = b.add_derived_level(grouped, 'grouped_sector', ['strategy', 'sector'], new_sector)
norm = lambda x: arb.normalise_allocations(x).round(2).to_frame('Alloc')

# ## Three Level Allocations

norm(arb.allocations_at_level(grouped, 'three_level_grouping'))

# ## Eight Level Allocations

norm(arb.allocations_at_level(grouped, 'eight_level_grouping'))

# ## Grouped Sector Allocations

norm(arb.allocations_at_level(grouped, 'grouped_sector'))

# ## Sector Allocations

norm(arb.allocations_at_level(grouped, 'sector'))

# ## Grouped Sector/Three Level Cross-Allocations

(arb.allocations_at_level(grouped, ['grouped_sector', 'three_level_grouping']).groupby(level='grouped_sector', axis=0).apply(lambda s: s / s.sum()) * 100).to_frame('Alloc').round(2).unstack().fillna('-')

# ## Sector/Three Level Cross-Allocations

(arb.allocations_at_level(grouped, ['sector', 'three_level_grouping']).groupby(level='sector', axis=0).apply(lambda s: s / s.sum()) * 100).to_frame('Alloc').round(2).unstack().fillna('-')
