#  Brief documentation
Instructions for deploying the service application via kubernetes on YandexCloud.

## Installations

### Docker CLI installation
Install Docker Engine
```commandline
 sudo apt-get update
 sudo apt-get install docker-ce docker-ce-cli containerd.io
```

### YandexCloud CLI
```commandline
curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
```

### Kubernetes CLI
Download the latest release with the command:
```commandline
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
```

Download the kubectl checksum file:
```commandline
curl -LO "https://dl.k8s.io/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256"
```

Validate the kubectl binary against the checksum file:
```commandline
echo "$(<kubectl.sha256)  kubectl" | sha256sum --check
```

Install kubectl:
```commandline
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

Testing:
```commandline
kubectl version --client
```

###  Argo CLI
Download the binary:
```commandline
curl -sLO https://github.com/argoproj/argo-workflows/releases/download/v3.3.0-rc10/argo-linux-amd64.gz
```

Unzip:
```commandline
gunzip argo-linux-amd64.gz
```

Make binary executable:
```commandline
chmod +x argo-linux-amd64
```

Move binary to path:
```commandline
mv ./argo-linux-amd64 /usr/local/bin/argo
```

Test installation:
```commandline
argo version
```

## Connect yc to kubbectl
Add Kubernetes cluster credentials to the kubectl configuration file:
```commandline
yc managed-kubernetes <cluster-name> get-credentials test-k8s-cluster --external
```

Check:
```commandline
kubectl config view
```

## Project pipeline
1. Сreate docker images for all the necessary processes.
2. Push images to container registry:
```commandline
docker tag <image-name> <regisrty-way>
docker push <regisrty-way>
```
3. Create yaml-manifest with a workflow description of the pipeline service.
4. Testing workflow with command:
```commandline
argo submit --watch <yaml-manifest>
```
5. Run the manifest on a schedule with CronWorkFlow:
```commandline
argo cron create <cron-yaml>
```

## Build argo workflow process
Example manifest with a directed-acyclic graph (DAG):
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
    generateName: dag-example-
spec:
    entrypoint: pipeline
    templates:
        - name: pipeline
          dag:
            tasks:
            - name: prep
              template: python-preparing-script
              
            - name: dics-update0
              dependencies: [prep]
              template: python-updating-script0
            - name: prediction0
              dependencies: [dics-update0]
              template: python-prediction-script0

              
        - name: python-preparing-script          
          script:
              image: <regisrty-way>
              command: [bash]
              source: |
                  python <script0> --<params>
              resources:
                  requests:
                      memory: 12Gi
                  limits:
                      memory: 16Gi  
                  
        - name: python-updating-script0
          script:
              image: <regisrty-way>
              command: [bash]
              source: |
                  python <script1> --<params>
              resources:
                  requests:
                      memory: 12Gi
                  limits:
                      memory: 16Gi
               
        - name: python-prediction-script0
          script:
              image: <regisrty-way>
              command: [bash]
              source: |
                  python <script2> --<params>
              resources:
                  requests:
                      memory: 12Gi
                  limits:
                      memory: 16Gi
```


## refs
* Argo: https://github.com/argoproj/argo-workflows/blob/master/examples/README.md#kubernetes-resources
* Memory limits: https://habr.com/ru/company/nixys/blog/480072/
* Official memory limits doc: https://kubernetes.io/docs/tasks/administer-cluster/manage-resources/memory-default-namespace/
* About pods: https://kubernetes.io/docs/concepts/workloads/pods/
* Cron Workflows in Argo: https://argoproj.github.io/argo-workflows/cron-workflows/


## some useful commands
1. list of nodes : `kubectl get nodes`
2. info by some node : `kubectl describe node <NODE>`
3. memory usege by nodes: `kubectl top nodes`
4. all workflows : `argo list -A`
5. run python script from docker-image like in bash: `docker run -it <image-name> <params-like-bash>`
6. all cron workflows : `argo cron list`
7. info about need cron workflow : `argo cron get <cron-workflow-name>`
8. get the node on which a pod is running : `kubectl get pod -o=custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName --all-namespaces`
