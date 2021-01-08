load("//bazel/rules:maven_download.bzl", "download_artifacts")

filegroup(
    name="checkedin_data",
    srcs=glob(["**/*"]),
)

ARTIFACTS = {
    "HG00096.g.vcf": "78a7445f923c9b1b8936e72132fd70f614642711d1018162255ec89f3d09943e",
    "HG00268.g.vcf": "d4fa07999e642349e7e451abbcc66f256904c25bb213947357dd0ea4e2e6e5c1",
    "NA19625.g.vcf": "68c84e2e80d894e53f889ade1f65f00e7b64e1072b42343d8e417abaa5e8452d",
    "genotyped.vcf": "328e563b4e373f2d7f00ba9a5d700484c18dd6697f5d30d112738eadce20c551",
}

download_artifacts(
    file_pairs = ARTIFACTS
)

filegroup(
    name = "joint",
    srcs = ARTIFACTS.keys() + ["checkedin_data"],
    visibility=["//hls/common:__subpackages__"],
)
