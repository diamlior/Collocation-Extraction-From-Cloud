# Collocation Extraction From Google 2grams:

Ravid Hansav
Lior Diamant


### Assignment:
In this assignment, we were requested to extract [collocations](https://en.wikipedia.org/wiki/Collocation) from the Google 2-grams dataset using
Amazon Elastic Map Reduce.
We used the [log likelihood ratio](https://en.wikipedia.org/wiki/Likelihood_function#Log-likelihood) in order to determine whether a given pair of
ordered words is a collocation.

### Collocations:
A collocation is a sequence of words that co-occur more often than would be expected by chance.
The identification of collocations - such as 'crystal clear', and 'cosmetic surgery' - is essential
for many natural language processing and information extraction applications.


### How to run?
The jar to execute is in the cloud.
Run "src\main\java\RunCollocation.java" with a language as an argument ("heb"/"eng").
This will create a new EMR cluster and will run all the needed steps.
The output will be located in "s3://collocation-ds/heb_final" or "s3://collocation-ds/eng_final".


### Our outputs:
The eng and heb outputs that we got and the good/bad examples of the program outputs are at "outputs" directory in this repository.

### Statistics:
For Hebrew -> 28,358,863 key-value pairs were sent from the mapper to the reducer, a total of 1.1GB.
For English -> 5,302,712,432 key-value pairs were sent from the mapper to the reducer, a total of 115GB.
