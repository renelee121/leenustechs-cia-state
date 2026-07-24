# leenustechs-cia-state
Project for an open source students state machine
```
leenustechs-cia-state
├─ LICENSE
├─ README.md
├─ pom.xml
├─ src
│  ├─ main
│  │  ├─ java
│  │  │  └─ mx
│  │  │     └─ com
│  │  │        └─ leenustechs
│  │  │           ├─ App.java
│  │  │           └─ ciaState
│  │  │              ├─ business
│  │  │              │  ├─ adapters
│  │  │              │  │  ├─ in
│  │  │              │  │  │  ├─ ApiInterceptor.java
│  │  │              │  │  │  └─ KafkaListenerAdapter.java
│  │  │              │  │  └─ out
│  │  │              │  │     └─ KafkaProducerAdapter.java
│  │  │              │  ├─ repositories
│  │  │              │  │  └─ EventStateRepository.java
│  │  │              │  ├─ services
│  │  │              │  │  ├─ CommandDispatcherService.java
│  │  │              │  │  ├─ EventStateService.java
│  │  │              │  │  └─ impl
│  │  │              │  │     ├─ CommandDispatcherServiceImpl.java
│  │  │              │  │     └─ EventStateServiceImpl.java
│  │  │              │  ├─ useCases
│  │  │              │  │  └─ LoginUseCase.java
│  │  │              │  └─ utils
│  │  │              │     ├─ commons
│  │  │              │     │  ├─ CustomDeserializer.java
│  │  │              │     │  ├─ CustomSerializer.java
│  │  │              │     │  ├─ EventOperation.java
│  │  │              │     │  └─ StageUtils.java
│  │  │              │     ├─ exceptions
│  │  │              │     │  ├─ EmptyOperationResponseException.java
│  │  │              │     │  ├─ TransactionNotFoundException.java
│  │  │              │     │  └─ handlers
│  │  │              │     └─ mappers
│  │  │              │        ├─ CommonModelMapper.java
│  │  │              │        ├─ EventStateInputMapper.java
│  │  │              │        └─ EventStateModelMapper.java
│  │  │              ├─ config
│  │  │              │  ├─ FeingConfig.java
│  │  │              │  ├─ InterceptorConfig.java
│  │  │              │  ├─ JacksonConfig.java
│  │  │              │  ├─ KafkaConfig.java
│  │  │              │  └─ RedisConfig.java
│  │  │              ├─ models
│  │  │              │  ├─ CommonModel.java
│  │  │              │  ├─ Event.java
│  │  │              │  ├─ EventStateModel.java
│  │  │              │  ├─ constants
│  │  │              │  │  └─ KafkaTopics.java
│  │  │              │  ├─ entities
│  │  │              │  │  └─ EventStateEntity.java
│  │  │              │  ├─ records
│  │  │              │  │  └─ WorkflowStage.java
│  │  │              │  ├─ requests
│  │  │              │  │  └─ CommonModelRequest.java
│  │  │              │  ├─ responses
│  │  │              │  │  ├─ CommonModelResponse.java
│  │  │              │  │  └─ EventStateResponse.java
│  │  │              │  └─ types
│  │  │              │     ├─ OperationType.java
│  │  │              │     ├─ StepStatus.java
│  │  │              │     ├─ StepType.java
│  │  │              │     └─ TransactionStatus.java
│  │  │              └─ rest
│  │  └─ resources
│  │     ├─ application.properties
│  │     └─ templates
│  │        └─ hello.html
│  └─ test
│     ├─ java
│     │  └─ mx
│     │     └─ com
│     │        └─ leenustechs
│     └─ resources
└─ target
   ├─ classes
   │  ├─ application.properties
   │  ├─ mx
   │  │  └─ com
   │  │     └─ leenustechs
   │  │        └─ ciaState
   │  │           ├─ business
   │  │           │  ├─ adapters
   │  │           │  │  ├─ in
   │  │           │  │  └─ out
   │  │           │  ├─ repositories
   │  │           │  ├─ services
   │  │           │  │  └─ impl
   │  │           │  ├─ useCases
   │  │           │  └─ utils
   │  │           │     ├─ commons
   │  │           │     ├─ exceptions
   │  │           │     └─ mappers
   │  │           ├─ config
   │  │           └─ models
   │  │              ├─ constants
   │  │              ├─ entities
   │  │              ├─ records
   │  │              ├─ requests
   │  │              ├─ responses
   │  │              └─ types
   │  └─ templates
   │     └─ hello.html
   ├─ generated-sources
   │  └─ annotations
   │     └─ mx
   │        └─ com
   │           └─ leenustechs
   │              └─ ciaState
   │                 └─ business
   │                    └─ utils
   │                       └─ mappers
   │                          ├─ CommonModelMapperImpl.java
   │                          ├─ EventStateInputMapperImpl.java
   │                          └─ EventStateModelMapperImpl.java
   ├─ generated-test-sources
   │  └─ test-annotations
   ├─ maven-archiver
   │  └─ pom.properties
   ├─ maven-status
   │  └─ maven-compiler-plugin
   │     ├─ compile
   │     │  └─ default-compile
   │     │     ├─ createdFiles.lst
   │     │     └─ inputFiles.lst
   │     └─ testCompile
   │        └─ default-testCompile
   │           ├─ createdFiles.lst
   │           └─ inputFiles.lst
   └─ test-classes

```