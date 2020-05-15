package mx.gob.cultura.endpoint.oai.transformers;

public interface OAITransformer<S, D> {
    D transform(S source);
}