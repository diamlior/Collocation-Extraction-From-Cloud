Collocation Extraction From Google 2grams:

By:
Ravid Hansav, 318522133, ravidhan
Lior Diamant, 313240756, liordia

How to run?
The jar to execute is in the cloud.
Run "src\main\java\RunCollocation.java" with a language as an argument ("heb"/"eng").
This will create a new EMR cluster and will run all the needed steps.
The output will be located in "s3://collocation-ds/heb_final" or "s3://collocation-ds/eng_final".


Our outputs:
The eng and heb outputs that we got and the good/bad examples of the program outputs are at "outputs" directory in this repository.

Statistics:
For Hebrew -> 28,358,863 key-value pairs were sent from the mapper to the reducer, a total of 1.1GB.
For English -> 5,302,712,432 key-value pairs were sent from the mapper to the reducer, a total of 115GB.
