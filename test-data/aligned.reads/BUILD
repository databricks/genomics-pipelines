load("//bazel/rules:maven_download.bzl", "download_artifacts")

filegroup(
    name="checkedin_data",
    srcs=glob(["**/*"]),
)

ARTIFACTS = {
    "bwa_mem_single_end.bam": "4c08fd1a86c3897fe4de2a1f3a67c08cb18ba273bc025daff3b8305208b8cf0e",
    "bwa_mem_paired_end.bam": "627cd554a8477426d84a5d6fc395e650b6f26c09dd4b8545d5728f5ede00d699",
}

download_artifacts(
    file_pairs = ARTIFACTS
)

filegroup(
    name = "aligned.reads",
    srcs = ARTIFACTS.keys() + ["checkedin_data"],
    visibility=["//hls/common:__subpackages__"],
)
