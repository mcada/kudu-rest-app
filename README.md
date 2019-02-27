# Rest api for testing syndesis kudu connector

Rest api was necessary due to kudu client RPC calls returning non-resolvable IP from outside of syndesis.

### Building

The API can be built with

    mvn clean install

### Running the api in Kubernetes/Openshift

It is assumed a running Kubernetes platform is already running. If not you can find details how to [get started](http://fabric8.io/guide/getStarted/index.html).

Assuming your current shell is connected to Kubernetes or OpenShift so that you can type a command like

```
kubectl get pods
```

or for OpenShift

```
oc get pods
```

Then the following command will package this app and run it on Kubernetes/Openshift:

```
mvn fabric8:run
```

To list all the running pods:

    oc get pods

Then find the name of the pod that runs this quickstart, and output the logs from the running pods with:

    oc logs <name of pod>

You can also use the [fabric8 developer console](http://fabric8.io/guide/console.html) to manage the running pods, and view logs and much more.

### Save image into local docker

```
mvn clean package fabric8:build -Dfabric8.mode=kubernetes -DskipTests
```
