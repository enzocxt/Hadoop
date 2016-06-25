### Hadoop
This project works on GPS tracks of taxis in [San Francisco][1]. Several datasets need to be dealt with, which are briefly introduced in following. More details about their format are given later, in the corresponding sections. The [datasets][2] are available online. 
- all.segments: The complete GPS tracks (including intermediary points) from May 2008 to January 2011. Each record is a segment with two end points (start position, end position), two time stamps (start date, end date) and two taxi states (empty/full) for the beginning and for the end of the segment. Consecutive segments can be concatenated to form complete trips. The dataset contains about 306 million segments and weighs 28GiB.
- 2010\_03.segments: Follows the same specification as the previous dataset, but only contains the segments started or finished in March 2010. The dataset still contains about 19 million segments and weighs 1.2GiB.
- 2010\_03.trips: Contains trips constructed from the segments in the previous dataset. Each trip is simply represented by its two end points (i.e. there is no intermediary position).
- Taxi\_706.segments: This dataset contains 19237 segments from taxi number 706, exclusively. This dataset is useful for debugging. 
The datasets are provided (almost) as such: they are not sorted, and they contain errors and misformatted records. Dealing with this type of data is part of the project. 
#### Trip length distribution
First, we are interested in computing a simple statistic: the distribution of trip lengths. We will compute this distribution for the trips in the 2010\_03.trips preprocessed dataset. 
For this exercise only, we will assume that the trip distance is the distance between the two end points of the trip. This information is easy to compute from 2010 03.trips, as it contains descriptions of the trips without the intermediary segments. In this dataset, each line has the following format (represented here on two lines): 
\<taxi-id\> \<start date\> \<start pos (lat)\> \<start pos (long)\> …
… \<end date\> \<end pos (lat)\> \<end pos (long)\>
To compute the geographical distance between two coordinates, you can use a simple [flat-surface formula][3], which will give a reasonable approximation for this dataset because the distances are not too large (but remember that these formulæ are not always appropriate for larger distances).
#### Computing airport ride revenue
A significant number of taxi rides pass through the San Francisco airport. Assume that taxi companies have to pay for an expensive license for this airport access. A company may then be interested in knowing exactly how much they earn from these airport rides, to know whether paying the license is actually worth it. 
This part of the project gives an estimate of the revenue coming from airport rides, based on the GPS tracking data. The estimate should be as accurate as possible, which means that you should consider all data, i.e. sampling is not an option. 
This part of the project consists of two steps, which we describe next: (1) reconstructing trips from segments, (2) computing the revenue obtained from these trips. 
##### Reconstructing trips
First, reconstruct complete trips from the ride segments. The .segments files contain the complete GPS tracks decomposed into segments. A segment is simply a pair of geographical coordinates. 
The sampling rate is generally 1 minute, although there can be larger gaps. Each line from these datasets has the following format (represented here on two lines): 
\<taxi-id\>, \<start date\>, \<start pos (lat)\>, \<start pos (long)\>, \<start status\>... 
...\<end date\> \<end pos (lat)\> \<end pos (long)\> \<end status\> 
Note on erroneous data-points: GPS points and recording devices are far from 100% reliable. Erroneous records will ultimately lead to erroneous results. One possible way to eliminate trips including erroneous data points, is to use a simple heuristic. For example, you can eliminate trips that include at least one segment with an average speed above 200km/h. 
##### Computing the revenue
In this step, use the output of the previous component to compute the total revenue obtained from airport trips. We consider airport trip rides as those that pass through a circle with the airport as center, and a radius of 1km. The airport is located at 37.62131 N, -122.37896 W. To calculate trip revenue, you can use a simple [formula][4] that combines a starting fee of $3.5 with an additional $1.71 per kilometer. 





[1]:	http://stamen.com/work/cabspotting/ "San Francisco"
[2]:	https://people.cs.kuleuven.be/~toon.vancraenendonck/bdap_files/ "datasets"
[3]:	https://en.wikipedia.org/wiki/Geographical_distance
[4]:	http://www.numbeo.com/taxi-fare/city%20result.jsp?country=United+States&city=San+Francisco%2C+CA