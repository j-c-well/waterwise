# WaterWise Corrections Engine

## Core Principle
Corrections are additive and profile-driven.
Universal rules apply to all households.
Household-specific rules only fire when the user
has confirmed they have that appliance/fixture.

## Data Layer
- Metron classifies intervals natively into:
  Toilet, Sink, Shower, WashingMachine, Other, Leak
- Metron accuracy: good for toilet/sink/shower,
  poor for dishwasher (often OTHER), poor for bath
- Timestamps: local MT time stored in UTC field,
  display as-is, no offset conversion needed
- consumptionDate: always YESTERDAY in MT time

## Rule Hierarchy

### Universal rules (apply to all users, no profile needed):
- Rule 3: Dual flush split — only if toiletType confirmed
- Rule 4: Sustained toilet → shower reclassification
- Rule 5: Bidet detection — only if bidetSeat confirmed
- Rule 6: Bath detection (≥20G, ≥10min, ≥1.8GPM, no bidet flanking)
- Rule 7: Continuous WashingMachine → shower reclassification

### Profile-driven rules (only fire with confirmed appliance):
- Rule 1: Disabled — too many false positives
- Rule 2: Dishwasher window scan
  (OTHER only, 6pm–6am default window, only if dishwasher.confirmed === true)
- Rule 8: Signature matching (see below)

### Dishwasher detection window:
- Default: 6pm-6am (evening through overnight)
- Configurable via householdProfile.dishwasher.runWindow
  e.g. { start: "18:00", end: "06:00" }
- Only fires if householdProfile.dishwasher.confirmed === true
- Minimum cluster: ≥ 2.5G total, ≥ 45 min span, no gap > 5 min
- Sanity floor: rejects anything < 2.0G or < 30 min

## Rule 8 — Signature Learning

After a user confirms an anomaly in the timeline, a flow signature
is extracted and stored:

```
{
  category,        // 'shower' | 'bath' | 'dishwasher' | 'sink' | 'laundry'
  timeOfDay,       // 'morning' | 'afternoon' | 'evening' | 'overnight'
  startHour,       // local MT hour (0–23)
  totalGallons,
  durationMin,
  avgFlowGPM,
  confirmedAt,
  confirmedBy: 'user'
}
```

Stored at: `waterwise:{userId}:signatures` (rolling, last 50)

Rule 8 runs last in the pipeline. For each group of consecutive
OTHER/UNKNOWN/WASHING_MACHINE intervals:
- Score 1 point each for: totalGallons within 40%, durationMin within 40%,
  avgFlowGPM within 40%, timeOfDay bucket matches
- 4/4 → confidence: high → reclassify
- 3/4 → confidence: medium → reclassify
- <3 → no reclassification

## Known Issues
- Dishwasher shows 0 when Metron puts it in OTHER
  and profile.dishwasher.confirmed is not set
- Rule 2 restricted to OTHER/UNKNOWN to prevent
  stomping shower intervals
