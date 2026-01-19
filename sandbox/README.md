# MISO LRZ1 Load Forecast Error Analysis: A Risk-Based Approach

**Location**: Minnesota (LRZ1)
**Analysis Time Frame**: January 2023 - January 2026
**Data Sources**: MISO RTDIP, KMSP Weather Station

---

## Executive Summary

MISO's load forecasts for Minnesota (LRZ1) show large directional bias patterns which can create asymmetric financial risk for local utilities. During extreme cold events (< -10°F) MISO has under-forecasted in 76% of hourly cases. Under-forecasted events in extreme cold carry a mean absolute error of 359 MW which can expose utitlities to scarcity pricing risk. Conversely, during extreme heat (> 85°F) while MISO has under-forecasted 61% of cases the mean absolute error for over-forecasted cases was 493 MW, which can lead to loss through make-whole payments.

**Key Finding**: Standard metrics (RMSE/MAPE) fail to capture directional bias, but **direction is the primary driver of financial risk**. Under-forecasting during scarcity carries **10x or greater cost asymmetry** (based on MISO LRZ1 price spikes during Jan 2024 cold snap). Additionally, under-forecasting risks **uplift charges** which are non-recoverable (FERC 561 / RUS 580) and directly impact utility margins. The spiked energy costs (FERC 555) are recoverable but result in higher prices for customers.

*Note: Detailed scarcity pricing analysis will be conducted in Phase 2 using MISO settlement data.*

---

## Phase 1: When and Where Do Forecast Errors Occur?

### Directional Error Analysis by Temperature Bucket

**Figure 1a: Directional Bias by Temperature**
![Under-forecast proportion by temperature](plots/under_forecast_by_temp_bucket.png)

**Figure 1b: Mean Absolute Error by Temperature and Direction**  
![MAE in MW by temperature bucket](plots/mae_by_temp_bucket_and_direction.png)

*Key Insight: Extreme cold events show systematic under-forecast bias (76% of errors), while extreme heat reveals model uncertainty with 493 MW over-forecast magnitude. Both patterns create asymmetric cost exposure that standard MAPE does not convey.*



- **Extreme Cold (< -10°F)**: 76% under-forecast errors, under-forecast mean error of 359 MW.
- **Extreme Heat (> 85°F)**: 61% under-forecast errors, under-forecast mean error of 354 MW and over-forecast mean error of 493 MW.
- **Cold to Mild (10°F - 70°F)**: 50-57% under-forecast errors, mean error ranges from 195 - 264 MW.


