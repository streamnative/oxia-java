package io.streamnative.oxia.client.api;

public record PutOptions(Long expectedVersionId, boolean ephemeral) {

    private static final PutOptions None = new Builder().build();

    public static Builder builder() {
        return new Builder();
    }

    public static PutOptions none() {
        return None;
    }

    public static PutOptions keyNotExists() {
        return new PutOptions(Version.KeyNotExists, false);
    }

    public static PutOptions expectedVersion(long versionId) {
        return new PutOptions(versionId, false);
    }

    public static class Builder {

        private Long versionId;
        //        private boolean ephemeral;

        Builder(Long versionId, boolean ephemeral) {
            if (ephemeral) {
                throw new UnsupportedOperationException();
            }
            this.versionId = versionId;
            //            this.ephemeral = ephemeral;
        }

        Builder() {
            this(null, false);
        }

        public PutOptions.Builder keyDoesNotExist() {
            versionId = Version.KeyNotExists;
            return this;
        }

        public PutOptions.Builder unconditional() {
            versionId = null;
            return this;
        }

        public PutOptions.Builder expectedVersionId(long versionId) {
            this.versionId = versionId;
            return this;
        }

        public PutOptions.Builder ephemeral() {
            throw new UnsupportedOperationException();
            //            ephemeral = true;
            //            return this;
        }

        public PutOptions build() {
            //            return new PutOptions(versionId, ephemeral);
            return new PutOptions(versionId, false);
        }
    }

    public Builder toBuilder() {
        return new Builder(expectedVersionId, ephemeral);
    }
}
