# Project Notes

## Live Trading Parameters

Current `config.yaml` live values such as:

- `strategy.bet_size_usd`
- `strategy.edge_threshold_pct`
- `risk.max_live_orders_per_day`
- `risk.max_live_notional_usd_per_day`
- `risk.max_directional_exposure_usd`
- `risk.max_total_directional_exposure_usd`
- `strategy.fat_tail_dampening`
- `strategy.max_win_prob`
- `strategy.adverse_selection_haircut`
- `strategy.fill_adverse_coeff`

are treated as `live pilot` parameters, not production capital deployment parameters.

Before any larger-scale real-money trading, these values must be explicitly re-evaluated based on:

- wallet size
- acceptable daily risk budget
- real fill statistics
- real exit / settlement / redeem behavior
- observed drawdown and live PnL behavior

Do not assume the current pilot settings should be carried forward unchanged into full production live trading.
