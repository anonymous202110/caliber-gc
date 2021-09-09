# caliber-gc
This is an anonymous repository for the project "Caliber-GC: A Causally Consistent Space Efficient Geo-Replicated Distributed Key-value Store."

## Caliber-GC is built on top of CausalSpartanX to perform garbage collection.
Follwing steps needs to be followed to run the Caliber-GC:
  1. Install eclipse.
  2. Import client and server of any of the 2 algorithms as 2 separate java projects running in Java 1.8 in eclipse.
  3. Add the JAR files from lib folder into the project builds.
  4. Run the server's main class.
  5. Export the server as a Runnable Jar and the client as a JAR.
  6. Similar to the server, create a new java project with cluster-manager source and add the libraries from its lib.
  7. Run the cluster-manager's main class and export the ClusterManager project as a Runnable Jar.
  8. Now follow the remaining steps as per the DKVF framework - Create a cluster and an experiment using cluster designer in https://github.com/roohitavaf/DKVF.
  9. Once a cluster and an experiment have been created, use the new ClusterManager jar with the old DKVF manager commands to run the experiments.
