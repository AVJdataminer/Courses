```python
nomin = ['Rank', 'Well Being', 'Work', 'Environment']
quantiles = [0.25, 0.75]

cut_off_points = []
for name in nomin:
    quants = imputed.approxQuantile(name, quantiles, 0.05)

    IQR = quants[1] - quants[0]
    cut_off_points.append((name, [
        quants[0] - 1.5 * IQR,
        quants[1] + 1.5 * IQR,
    ]))

cut_off_points = dict(cut_off_points)

## Flag outliers with boolean

outliers = imputed.select(*['id'] + [
       (
           (df[f] < cut_off_points[f][0]) |
           (df[f] > cut_off_points[f][1])
       ).alias(f + '_o') for f in nomin
  ])
  
## join df with outlier flags

with_outliers_flag = df.join(outliers, on='Id')

(
    with_outliers_flag
    .filter('Environment_o')
    .select('Id', 'Well Being', 'Work', 'Environment)
    .show()
)
  
## Final Data Frame with Outliers removed
no_outliers = (
    with_outliers_flag
    .filter('!Environment_o')
    .select(df.columns)
)
```
