# Research design

## Question

Evaluate placement and scaling of a containerized service in a 5G MEC testbed as UE demand and its distribution between cells change. A face-detection service is included, but the placement objective does not assume a particular application protocol.

## Comparisons

Use the same service, requests, routes, replica bounds, eligible nodes and measurement windows for each matched comparison. Choose the latency SLO separately and freeze it before evaluation.

1. Compare a conventional Kubernetes autoscaler with the proposed scaling policy using the same scheduler and placement constraints.
2. Compare the proposed scaler with and without mobility-aware placement to isolate the placement contribution.
3. Include fixed-replica controls to show the service-level performance and resource cost of persistent capacity.

Specify the autoscaler metric and configuration explicitly. The placement controller and another autoscaler must not simultaneously write the same Deployment's replica count.

## Workloads and measurements

Run clients on the NUC through UERANSIM UEs. Use ordered load ramps and changing UE distribution across cells; retain repeated tiers as distinct phases. Record actual UE attachment, routing, node placement, replica readiness, policy actions and phase boundaries. Measure end-to-end request latency, all-attempt SLO attainment, throughput, failures, and allocated replica-seconds; record CPU, memory, network and other resources when available. A closed-loop UE schedule is not an equal offered request-rate trace.

Repeat independent matched runs, counterbalance policy order, retain failed attempts and predeclare exclusion rules. Requests from one run are not independent experimental repetitions. Report placement and scaling separately; changes to routing or multiple policies in the same comparison confound attribution.

## Safety and provenance

Repository preparation does not authorize a campaign or changes to the live cluster. Before an experiment, inspect the target output directory and avoid overwriting results; capture current state and document routing, workload and phase schedule, policies, replica budget, scheduling constraints, expected interruptions and recovery. Ensure only one writer owns replicas. Preserve results until transfers are verified, restore captured configuration, and synchronize relevant sources as required by the testbed operator procedures.

Initial placement must not be hard-pinned to one node.
