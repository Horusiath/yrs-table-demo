# Yrs table demo

This is a simple demo of a collaborative table capabilities using [yrs](https://docs.rs/yrs/latest/yrs/) library.
As an example we're using a Uber data set, which can be downloaded
from [Kaggle](https://www.kaggle.com/datasets/yasserh/uber-fares-dataset#).

This data set contains 200 000 rows and in this demo we can see how we're able to import it into Yrs document structure
and serialize it in subsecond speeds.

## Example

```
> cargo run --release

imported 1800000 cells in 814.888708ms
encoded 1800000 cells (200000 rows x 9 columns) in 100.70175ms: 58213277 bytes (original file size: 23458139 bytes)

# printout of the first 10 rows
```