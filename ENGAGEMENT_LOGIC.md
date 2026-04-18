# WaterWise Engagement Logic
## Behavioral Science Framework for Email & In-App Nudges

*Version 1.0 — April 2026*
*For review by behavioral science advisors*

---

## 1. Core Behavioral Principles

### 1.1 The Engagement Hierarchy
Not all actions are equal. This framework prioritizes interventions by their behavioral leverage — the degree to which they change what someone does tomorrow, not just what they know.

**Tier A — Behavior change (highest leverage)**
Actions that directly reduce water consumption: cutting an irrigation cycle, shortening a shower, fixing a leak. These are the end goal.

**Tier B — Model improvement (medium leverage)**
Confirming an event classification ("yes, that was my dishwasher"). Doesn't directly save water but improves data accuracy, which enables better future nudges. Also drives app engagement which sustains the behavior change loop.

**Tier C — Profile building (medium leverage)**
Adding household details (appliance type, toilet type, members). Improves classification accuracy and personalizes future communications. One-time but high-value.

**Tier D — Social/competitive engagement (medium leverage)**
Assigning showers to household members, viewing the leaderboard. Leverages social comparison, one of the most reliable behavior change mechanisms in conservation literature.

**Tier E — Awareness (lowest leverage alone)**
Reading data, viewing the dashboard, opening the email. Necessary but not sufficient. Awareness without a specific action prompt rarely changes behavior.

### 1.2 The Crying Wolf Problem
Showing conservation tips every week trains users to ignore them. Habituation is the enemy of effective nudging. Tips must feel:
- **Rare** — only shown when genuinely warranted
- **Specific** — tied to their actual data, not generic advice
- **Timely** — shown when the user can still act, not after the fact

### 1.3 One Ask Per Communication
Research on behavioral nudges consistently shows that multiple simultaneous asks reduce compliance with any single ask. Every email and in-app prompt should have exactly one primary call to action.

### 1.4 Loss Aversion Over Gain Framing
"You're 1,200 gallons from Tier 2, which adds $4.80 to your bill" outperforms "You could save $4.80 by staying in Tier 1." Loss aversion is approximately 2x stronger than equivalent gain framing in conservation contexts.

### 1.5 Specific Over General
"Your evening showers average 18.4 gallons — 2x your morning showers" changes behavior. "Try to use less water in the shower" does not. Specificity signals that the system knows you, which builds trust and increases compliance.

---

## 2. User Lifecycle States

### State 0 — Newly Registered (Days 1-3)
**User mindset:** Curious, orienting, not yet invested.
**Primary goal:** First aha moment.
**What works:** A specific surprising insight from their data.
**What doesn't work:** Tips, challenges, overwhelming features.

### State 1 — Orienting (Days 4-14)
**User mindset:** Learning what the app does.
**Primary goal:** First meaningful action (event confirmation or profile addition).
**What works:** "Does this look right?" prompts. Profile completion nudges.
**What doesn't work:** Shower challenges (premature), generic tips.

### State 2 — Engaged (Days 15-60)
**User mindset:** The app is part of their routine.
**Primary goal:** Sustain engagement, introduce social features, drive behavior change.
**What works:** Leaderboard if members set up, tier alerts, disaggregation insights.
**What doesn't work:** Setup prompts they've already completed.

### State 3 — At Risk (30+ days since last app open or shower assignment)
**User mindset:** Slipped away. Not hostile, just forgot.
**Primary goal:** Re-engagement with something surprising.
**What works:** A teaser showing what they're missing. Competitive re-engagement hook.
**What doesn't work:** "We miss you" emails, reminders to do setup steps they've ignored.

### State 4 — Seasonal (Irrigation season May-Oct)
**User mindset:** Usage is higher, bills are higher, conservation matters more.
**Primary goal:** Irrigation behavior change — the single highest-leverage opportunity.
**What works:** Specific irrigation volume tracking, direct cost impact of reducing cycles.
**What doesn't work:** Indoor conservation tips during irrigation season (wrong lever).

---

## 3. Weekly Email Logic

### 3.1 Subject Line Priority
```
Priority 1 (Urgent): Spike detected
→ "Something unusual happened with your water yesterday 💧"

Priority 2 (Financial urgency): Approaching tier threshold
→ "1,200 gal from Tier 2 · act this week"

Priority 3 (Social/competitive): Leaderboard leader changed
→ "Cowboy took the Clean League lead 🏆"

Priority 4 (Curiosity): Unconfirmed event awaiting input
→ "Does this look right? · Apr 18"

Priority 5 (Default): Summary
→ "Joshua · 86 gal/day avg · Day 18 of 30"
```

### 3.2 Tier Display Logic
Show **current tier + next tier only**. Hide all other tiers.

When user is in Tier 3+, show current tier + next tier only (still just two).

Format:
```
Tier 1 ✅  $2.89/1k gal
[progress bar — 68% to Tier 2]

Tier 2 starts at 3,800 gal · $3.85/1k gal
At this pace: safely past this billing cycle
```

### 3.3 Conservation Tip Logic
Show tip ONLY when:
- `nudge === 'approaching'` (< 500 gallons from tier crossing)
- `nudge === 'in_tier_2'`
- `nudge === 'in_tier_3plus'`
- `spikeAlert === true`

When tip shows: it occupies the "one ask" slot. Do NOT also show a disaggregation question.
When tip does NOT show: omit the section entirely.

Tips must reference the user's actual context (irrigation vs indoor, specific appliances).

### 3.4 Disaggregation "Does This Look Right?" Logic
Show only when tip is NOT showing (one ask rule).

Event selection criteria:
1. classification is not toilet/sink/bidet
2. `needsConfirmation === true`
3. `confidence` between 0.60 and 0.85
4. `gallons > 5`
5. Never show same event twice (track in Redis per user)

Priority order:
1. Dishwasher (highest model value)
2. Bath vs shower ambiguity
3. Large unclassified OTHER > 15G
4. Washing machine that might be a shower
5. Any other unconfirmed event

Framing — always a question:
```
🔍 Does this look right?

Yesterday at 7:10 PM we detected a 26-gallon event
over 15 minutes. We think it was a shower —
but it could be something else.

[Looks right ✓]  [Something else →]
```

Both buttons deep-link to the timeline with that event highlighted.

### 3.5 Shower Challenge / Clean League Logic

State machine:

**State A — No members, tease not yet shown:**
Show setup tease once. Store `waterwise:{userId}:shower-tease-shown = true`.
```
🚿 Start a shower challenge
[Set up your household →]
```

**State B — No members, tease already shown:**
Show nothing. Don't nag.

**State C — Members set up, showers assigned within 30 days:**
Show real leaderboard. If gap < 1G between #1 and #2, add competitive nudge.
```
🚿 Clean League · April
🏆 Cowboy    6.4 gal avg · 4 min
   L dawg    6.5 gal avg · 5 min
   Pepperoni 7.3 gal avg · 5 min
"Pepperoni is 0.9G behind L dawg. Close enough to catch up."
```

**State D — Members set up, no assignments in 30+ days:**
Re-engagement tease only.
```
🚿 Your Clean League has gone quiet
You haven't assigned showers in 5 weeks.
[Assign this week's showers →]
```

### 3.6 Snowpack Section Logic
Show only when `snowpackSWEPct < 60`.

### 3.7 Full Email Structure
```
[Logo + header]
[Headline stat — one big number]
[Tier status — current + next only]
[CONDITIONAL: Conservation tip — only if nudge triggered]
[OR: Disaggregation question — only if no tip, only if event available]
[CONDITIONAL: Shower leaderboard — state-dependent]
[CONDITIONAL: Snowpack — only if < 60%]
[CTA button — personalized dashboard link]
[Footer]
```

---

## 4. In-App Prompt Logic

### 4.1 Anomaly Confirmation Cards
```
> 200G  → red/urgent border
50-200G → orange border
15-50G  → yellow border
10-15G  → gray/subtle border
< 10G   → no card (noise threshold)
```

Category suggestions by volume:
```
>= 200G AND duration < 30min  → Hot tub fill? Pool top-off?
>= 50G AND implied > 10 GPM  → Irrigation system?
10-50G AND duration > 15min   → Garden hose? Long shower? Bath?
5-20G AND duration 30-90min   → Laundry? Dishwasher?
```

### 4.2 Profile Completion Prompts
Show when profile has fewer than 2 confirmed appliances.
One question at a time. Never stack multiple profile questions.

### 4.3 Seasonal Transition Prompts
Surface April 15 — October 31.

Pre-season (Apr 15-30): Ask about irrigation system type.
Active season (May 1-Oct 31): Show irrigation fixture row priming awareness.

---

## 5. What We Are Not Building (and Why)

**No generic daily reminders** — daily notifications habituate within 2 weeks.

**No gamification points/badges** — shifts motivation from intrinsic to extrinsic, reducing persistence.

**No comparisons to strangers** — without careful matching, neighborhood comparison causes boomerang effect (below-average users increase use). Use household internal comparison only until sufficient sample size.

**No real-time alerts for every event** — creates noise, reduces signal salience.

---

## 6. Open Questions for Behavioral Science Review

1. **Re-engagement timing:** Is 30 days the right threshold for "disengaged"? Seasonal apps may need longer windows.

2. **Loss vs gain framing at Tier 1:** When safely in Tier 1 with no near-term risk, does loss framing create unnecessary anxiety?

3. **Social comparison matching:** Minimum sample size for valid neighborhood comparison? Key matching variables?

4. **Confirmation fatigue:** Optimal frequency for "does this look right?" prompts before users ignore them?

5. **Shower challenge timing:** Day 15+ for introduction — is this right, or should social features come earlier (day 7) while novelty is high?

6. **Drought level messaging:** Official designation vs plain language ("mild/moderate/severe")?

7. **Tier rate psychology:** Absolute cost ($4.80) vs rate increase ($3.85 vs $2.89/kgal)?

---

## 7. Data Sources

- WaterScope interval data (1-minute resolution, Metron-Farnier)
- Household profile (appliances, toilet type, members, irrigation)
- User confirmation history
- Shower log (member assignments, duration, gallons)
- Agent classification (Claude-generated with confidence scores)
- Snowpack data (SNOTEL Station 936, Loveland Basin)
- EMD tier structure and drought level

---

## 8. References

- Schultz, P.W., et al. (2007). The constructive, destructive, and reconstructive power of social norms. *Psychological Science.*
- Allcott, H. (2011). Social norms and energy conservation. *Journal of Public Economics.*
- Cialdini, R. (2003). Crafting normative messages to protect the environment. *Current Directions in Psychological Science.*
- EPA WaterSense residential end-use research (2016 update).
- Kahneman, D. & Tversky, A. (1979). Prospect Theory. *Econometrica.*
- Fogg, B.J. (2009). A behavior model for persuasive design. *Persuasive Technology Conference.*
