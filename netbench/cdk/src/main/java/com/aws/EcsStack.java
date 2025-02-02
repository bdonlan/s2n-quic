// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.aws;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.autoscaling.AutoScalingGroup;
import software.amazon.awscdk.services.servicediscovery.DnsRecordType;
import software.amazon.awscdk.services.servicediscovery.PrivateDnsNamespace;
import software.amazon.awscdk.services.stepfunctions.tasks.EcsRunTask;
import software.amazon.awscdk.services.stepfunctions.IntegrationPattern;
import software.amazon.awscdk.services.stepfunctions.tasks.EcsEc2LaunchTarget;

import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.EcsOptimizedImage;
import software.amazon.awscdk.services.ecs.AsgCapacityProvider;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.Ec2Service;
import software.amazon.awscdk.services.ecs.CapacityProviderStrategy;
import software.amazon.awscdk.services.ecs.NetworkMode;
import software.amazon.awscdk.services.ecs.PortMapping;
import software.amazon.awscdk.services.ecs.AmiHardwareType;
import software.amazon.awscdk.services.ecs.CloudMapOptions;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.LogDriver;

import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Peer;
import software.amazon.awscdk.services.ec2.Port;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

class EcsStack extends Stack {
    private String dnsAddress;
    private EcsRunTask ecsTask;

    public EcsStack(final Construct parent, final String id, final EcsStackProps props) {
        super(parent, id, props);

        String stackType = props.getStackType();
        String instanceType = props.getInstanceType();
        Vpc vpc = props.getVpc();
        Bucket bucket = props.getBucket();

        SecurityGroup sg = SecurityGroup.Builder.create(this, stackType + "ecs-service-sg")
            .vpc(vpc)
            .build();
        sg.addIngressRule(Peer.anyIpv4(), Port.allTraffic());

        Cluster cluster = Cluster.Builder.create(this, stackType + "-cluster")
            .vpc(vpc)
            .build();
        
        AutoScalingGroup asg = AutoScalingGroup.Builder.create(this, stackType + "-asg")
            .vpc(vpc)
            .instanceType(new InstanceType(instanceType))
            .machineImage(EcsOptimizedImage.amazonLinux2(AmiHardwareType.ARM))
            .minCapacity(0)
            .desiredCapacity(1)
            .securityGroup(sg)
            .build();

        AsgCapacityProvider asgProvider = AsgCapacityProvider.Builder.create(this, stackType + "-asg-provider")
            .autoScalingGroup(asg)
            .build();
        
        cluster.addAsgCapacityProvider(asgProvider);

        Ec2TaskDefinition task = Ec2TaskDefinition.Builder
            .create(this, stackType + "-task")
            .networkMode(NetworkMode.AWS_VPC)
            .build();

        Map<String, String> ecrEnv = new HashMap<>();
        ecrEnv.put("SCENARIO", props.getScenario());
        ecrEnv.put("PORT", "3000");  //Arbitrary port

        if (stackType.equals("server")) {
            PrivateDnsNamespace ecsNameSpace = PrivateDnsNamespace.Builder.create(this, stackType + "-namespace")
                .name(stackType + "ecs.com") //Arbitrary name
                .vpc(vpc)
                .build();

            task.addContainer(stackType + "-driver", ContainerDefinitionOptions.builder()
                .image(ContainerImage.fromRegistry(props.getEcrUri()))
                .environment(ecrEnv)
                .memoryLimitMiB(2048)
                .logging(LogDriver.awsLogs(AwsLogDriverProps.builder().streamPrefix(stackType + "-ecs-task").build()))
                .portMappings(List.of(PortMapping.builder().containerPort(3000).hostPort(3000)
                    .protocol(software.amazon.awscdk.services.ecs.Protocol.UDP).build()))
                .build());

            bucket.grantWrite(task.getTaskRole());

            CloudMapOptions ecsServiceDiscovery = CloudMapOptions.builder()
                    .dnsRecordType(DnsRecordType.A)
                    .cloudMapNamespace(ecsNameSpace)
                    .name("ec2serviceserverCloudmapSrv-UEyneXTpp1nx") //Arbitrary hard-coded value to make DNS resolution easier
                    .build();
            
            dnsAddress = ecsServiceDiscovery.getName();

            Ec2Service service = Ec2Service.Builder.create(this, "ec2service-" + stackType)
                .cluster(cluster)
                .taskDefinition(task)
                .cloudMapOptions(ecsServiceDiscovery)
                .capacityProviderStrategies(List.of(CapacityProviderStrategy.builder()
                    .capacityProvider(asgProvider.getCapacityProviderName())
                    .weight(1)
                    .build()))
                .desiredCount(1)
                .securityGroups(List.of(sg))
                .build();
        } else {
            ecrEnv.put("DNS_ADDRESS", props.getDnsAddress() + ".serverecs.com");
            ecrEnv.put("SERVER_PORT", "3000");
            ecrEnv.put("S3_BUCKET", bucket.getBucketName());

            task.addContainer(stackType + "-driver", ContainerDefinitionOptions.builder()
                .image(ContainerImage.fromRegistry(props.getEcrUri()))
                .environment(ecrEnv)
                .memoryLimitMiB(2048)
                .logging(LogDriver.awsLogs(AwsLogDriverProps.builder().streamPrefix(stackType + "-ecs-task").build()))
                .portMappings(List.of(PortMapping.builder().containerPort(3000).hostPort(3000)
                    .protocol(software.amazon.awscdk.services.ecs.Protocol.UDP).build()))
                .build()); 

            bucket.grantWrite(task.getTaskRole());

            ecsTask = EcsRunTask.Builder.create(this, "client-run-task")
                .integrationPattern(IntegrationPattern.RUN_JOB)
                .cluster(cluster)
                .taskDefinition(task)
                .launchTarget(EcsEc2LaunchTarget.Builder.create().build())
                .build();
        }
    }

    public String getDnsAddress() {
        return dnsAddress;
    }

    public EcsRunTask getEcsTask() {
        return ecsTask;
    }
}