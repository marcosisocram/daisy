<div align="center" width="100%">
    <img alt="A dog running with John Wick in a park with red flowers" src="/images/daisy.webp?raw=true" title="Logo"/>
</div>

## daisy
Projeto para a Rinha de Backend 3a feito de forma duvidosa

### Stack

- Java 21
- Undertow
- GraalVM
- Nginx
- SQLite

```shell
 ./mvnw native:compile
```

## Como gerar a imagem docker

```shell
 docker buildx build -t daisy:1.0.0 .
```

## Dificuldades
- Inconsistencia, devido ao uso de queue para inserir no sqlite


## Conhecimentos adquiridos

- SQLite
- Undertow
- K6
- Chaves GPG do GitHub



