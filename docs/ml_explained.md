# Machine learning in EtherLens, from first principles

EtherLens uses two models:

1. **KMeans clustering** to find unusual Ethereum addresses without any labels
   (`src/etherlens/batch/kmeans_anomaly.py`).
2. **Linear regression** to predict next hour's gas price
   (`src/etherlens/batch/analytics_types.py`, the "predictive" part of the four analytics types).

Both run on Spark MLlib, so they scale to millions of rows across the cluster.

---

## Part 1. Finding unusual addresses with KMeans

### What we are trying to do

There are 1,435,758 addresses with at least 5 transactions in January 2025. Most are ordinary
wallets. A few behave very differently: trading bots that send thousands of transactions,
exchanges, whales moving huge amounts. Nobody has labelled which is which. Learning without labels
is called **unsupervised learning**.

The idea: describe every address with a few numbers, group similar addresses together, and flag
the ones that do not fit any group well.

### Step 1: describe each address with features

From the gold table `address_features`, each address gets 8 numbers:

| Feature | Meaning | Why it helps |
|---|---|---|
| `log_tx_out` | how many transactions it sent | bots send a lot |
| `log_tx_in` | how many it received | exchanges and popular contracts receive a lot |
| `log_eth_out` | how much ETH it sent | whales move large values |
| `log_counterparties` | how many different addresses it dealt with | a wash-trading ring trades with only a few partners |
| `log_max_nonce` | its highest nonce | the nonce counts every transaction an account has ever sent; old busy bots have huge nonces |
| `avg_gas_gwei` | average gas price it paid | bots often pay unusual gas prices |
| `plain_share` | share of plain 21,000-gas ETH transfers | wallets send plain transfers; bots call contracts |
| `log_active_hours` | how many different hours of the day it was active | humans sleep; bots do not |

**Why `log1p`?** Counts like "transactions sent" range from 5 to millions. Without the log, one
giant address would dominate every distance. `log1p(x) = log(1 + x)` squeezes large values (and
works for 0).

### Step 2: put the features on the same scale

KMeans measures straight-line (Euclidean) distance. If one feature runs from 0 to 20 and another
from 0 to 1, the first one decides everything. `StandardScaler(withMean=True, withStd=True)`
rescales every feature to mean 0 and standard deviation 1, so each feature has a fair say.

The Spark pipeline is: `VectorAssembler` (8 columns → one vector) → `StandardScaler` → `KMeans`.

### Step 3: KMeans

KMeans splits the points into *k* groups:

1. Pick *k* starting centres (Spark uses a smart random start, `k-means||`).
2. Assign every address to its nearest centre.
3. Move each centre to the average of the addresses assigned to it.
4. Repeat 2–3 until the centres stop moving.

The result: every address has a cluster number and a centre it belongs to.

### Step 4: choose *k* with the silhouette score

We do not know how many kinds of address there are, so we try *k* = 4 … 10 and score each
clustering with the **silhouette**:

- For one point, *a* = average distance to the other points in its own cluster,
  *b* = average distance to the points of the nearest other cluster.
- silhouette = (*b* − *a*) / max(*a*, *b*). Close to 1: clearly in the right cluster.
  Around 0: on the border between clusters. Negative: probably in the wrong one.
- The score for the clustering is the average over all points.

On real data the scores were between 0.24 and 0.28 for every *k*, with the best at *k* = 10
(0.278). On the synthetic data the best was *k* = 6 (0.354). The real-data numbers mean the
clusters overlap a lot: Ethereum addresses do not fall into neat groups. That is an honest
finding, not a bug. On the cluster the silhouette is computed on a 5% sample, because it
compares points with each other and gets expensive on 1.4 million rows.

### Step 5: flag the unusual addresses

Two rules (`kmeans_anomaly.py`):

- **far_from_centroid**: the address is in the top 0.5% by distance to its own cluster's
  centre. It belongs to no group well.
- **rare_cluster**: the address sits in a cluster holding less than 1% of all addresses, a
  small group of oddballs.

On real data, 7,417 addresses were flagged (0.5%). They feed the daily report
(`flagged_active` rows) and the streaming Bloom filter.

One bug was found and fixed along the way: the 99.5th percentile was computed with
`approxQuantile(…, 0.01)`, a 1% allowed error. For a 0.5% tail that can return the maximum
distance, so nobody was flagged. The error was lowered to 0.001.

### Step 6: how do we know it works? The ground truth from mock data

Unsupervised learning has no labels to check against. That is why the synthetic (mock) data
exists: its generator **plants** anomalies on purpose (`src/etherlens/mock/entities.py`):

- 8 **bots**, each sending about 1% of all traffic, evenly around the clock, to 3 fixed
  contracts, with nonces of 200,000 or more,
- a **wash-trading ring** of 6 addresses passing about 50 ETH around in a circle
  (A → B → … → F → A), about 20 hops a day,
- 15 **whales**, each sending 1–5 transactions a day of 500–20,000 ETH.

We then measure:

- **Recall** = planted anomalies that were flagged / all planted anomalies.
  "Of the real anomalies, how many did we catch?"
- **Precision** = planted anomalies that were flagged / everything flagged.
  "Of what we flagged, how much was a real anomaly?"

Result on the small mock preset (`results/kmeans_eval_mock.json`):
**recall 1.00** (29 of 29, including all 8 bots; the spec asked for at least 0.8 for bots) and
**precision 0.25**.

Why is precision "only" 0.25? The rule flags a fixed budget of about 0.5% of addresses
(114 of 19,275). Only 29 true anomalies exist, so even a perfect detector would have at most
29/114 ≈ 0.25 precision at that budget. KMeans reached that ceiling while missing none. The other
85 flags are ordinary synthetic users at the extreme edges of their clusters. To raise precision
you would lower the budget, at the risk of missing real anomalies: the usual trade-off.

### Limits to mention honestly

- "Unusual" is not "malicious". A flagged address is far from the typical behaviour; a person
  still has to look at it.
- KMeans assumes round, similar-sized clusters. Real behaviour is messier (the low silhouette
  shows this).
- The features cover one month only; an address's history before January is invisible except
  through the nonce.

### Picture

`clusters_pca.png` shows the clusters squashed to 2 dimensions with **PCA** (principal
component analysis): it finds the two directions in which the 8 features vary most and plots the
addresses along them. Flagged addresses sit at the outer edges.

---

## Part 2. Predicting next hour's gas price

### The question

Given this hour's gas price and how busy it is, can we predict next hour's average gas price?
(Gas price is what a sender pays per unit of computation, in gwei = 10⁻⁹ ETH.)

### The model

`LinearRegression` in MLlib. Features: this hour's average gas, this hour's transaction count,
and the hour of day as 23 on/off columns (one-hot encoding with one hour dropped, so the model can
learn "15:00 is expensive"). The model fits a weighted sum of these features that best matches
the target `next_gas`.

### Chronological split, not random

The hours are put in time order: the first 80% train the model and the last 20% test it.

A random split would put, say, 14:00 on 20 January in training and 13:00 on 20 January in testing.
The model would then "know" the near future of the test hours, which is **leakage**, and the test
score would look better than real life. In real use you only ever know the past, so we test on the
future.

### How we measure error

- **RMSE** (root mean squared error): the typical size of a prediction error, in gwei. Big misses
  count extra because errors are squared before averaging.
- **MAE** (mean absolute error): the average size of the miss, in gwei. Less sensitive to a few
  big misses.
- **R²**: the share of the variation in gas price the model explains. 1 is perfect; 0 is no better
  than always predicting the average.

### Why a baseline matters

A model is only useful if it beats something simple. Our baseline is **persistence**: "next hour
will cost the same as this hour".

| Data | Model RMSE | Baseline RMSE | Verdict |
|---|---|---|---|
| Mock data | 0.208 gwei (R² 0.996) | 0.865 gwei | Model wins clearly: the generator has a fixed daily pattern |
| Real January 2025 | 2.64 gwei (R² 0.56) | 2.24 gwei | **Model loses** to the baseline |

The real-data result is the honest and interesting one. Gas prices on real Ethereum move almost
like a random walk from hour to hour: the best guess for the next hour is mostly the current hour.
The hour-of-day pattern exists (it drives the prescriptive result, cheapest at 04:00 UTC), but it
is too weak to beat persistence for next-hour prediction. Without the baseline we might have
reported R² = 0.56 as a success.

### How the four analytics types fit together

| Type | Question | EtherLens answer (real data) |
|---|---|---|
| Descriptive | What happened? | 37.3 M successful transactions, about 1.2 M a day; 34% plain ETH transfers |
| Diagnostic | Why? | Busier hours have higher gas (correlation 0.27 between hourly count and gas) |
| Predictive | What will happen? | Next-hour gas regression, which does not beat persistence |
| Prescriptive | What should we do? | Send at 04:00 UTC: 68.4% cheaper than the 15:00 UTC peak |
