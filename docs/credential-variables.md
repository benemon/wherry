# Variables read implicitly

<!-- Generated from internal/catalog by go test ./internal/catalog -run TestDocsListTheCatalogVariables -update. Do not edit. -->

A host reads these environment variables, and no others, from the process
that submits: when a pool's terms give one of the placed agent's credentials
one of them as its target and the host's configuration names no source for
that credential, the host takes the variable's value when it is set, and
the run gets it. See [Credentials from the submitting process](configuration-host.md#credentials-from-the-submitting-process)
for the rule and the [warning](security.md#credential-envelopes) on what
a set variable reaches. The list is the credential catalog's: wherry's
harness entries and the providers of the committed models.dev snapshot,
`internal/catalog/SOURCE` naming its fetch. A variable outside this list,
`GH_TOKEN` or `MODEL_API_KEY` for example, is read only through an explicit
source.

| Variable | Catalog entries |
| --- | --- |
| `302AI_API_KEY` | `302ai` |
| `ABACUS_API_KEY` | `abacus` |
| `ABLITERATEDLLM_API_KEY` | `abliteratedllm` |
| `ABLIT_KEY` | `abliteration-ai` |
| `ABOVE_API_KEY` | `above` |
| `ADAPTION_API_KEY` | `adaption` |
| `ADZUKI_API_KEY` | `adzuki` |
| `AGENTROUTER_API_KEY` | `agentrouter` |
| `AGNES_API_KEY` | `agnes` |
| `AI21_API_KEY` | `ai21` |
| `AIAND_API_KEY` | `aiand` |
| `AICORE_SERVICE_KEY` | `sap-ai-core` |
| `AIGROW_API_KEY` | `aigrow` |
| `AIHUBMIX_API_KEY` | `aihubmix` |
| `AIMLAPI_API_KEY` | `aimlapi` |
| `AINETCAFE_API_KEY` | `ainetcafe` |
| `AIPG_API_KEY` | `aipg` |
| `AISTUPIDLEVEL_API_KEY` | `aistupidlevel` |
| `AIXY_API_KEY` | `aixy` |
| `AI_GATEWAY_API_KEY` | `vercel` |
| `AI_ROUTER_API_KEY` | `ai-router` |
| `AKI_IO_API_KEY` | `aki-io` |
| `ALIBABA_CODING_PLAN_API_KEY` | `alibaba-coding-plan`, `alibaba-coding-plan-cn` |
| `ALIBABA_TOKEN_PLAN_API_KEY` | `alibaba-token-plan`, `alibaba-token-plan-cn` |
| `AMBIENT_API_KEY` | `ambient` |
| `AMD_API_KEY` | `amd` |
| `ANONROUTER_API_KEY` | `anonrouter` |
| `ANTHROPIC_API_KEY` | `anthropic` |
| `ANYAPI_API_KEY` | `anyapi` |
| `APIKO_API_KEY` | `apiko` |
| `ARCEE_API_KEY` | `arcee` |
| `ARK_API_KEY` | `volcengine` |
| `ARK_CODING_PLAN_API_KEY` | `volcengine-coding-plan` |
| `ATOMIC_CHAT_API_KEY` | `atomic-chat` |
| `AUGURE_API_KEY` | `augure` |
| `AURIKO_API_KEY` | `auriko` |
| `AUTARCHO_API_KEY` | `autarcho` |
| `AWS_ACCESS_KEY_ID` | `amazon-bedrock` |
| `AWS_BEARER_TOKEN_BEDROCK` | `amazon-bedrock` |
| `AWS_REGION` | `amazon-bedrock` |
| `AWS_SECRET_ACCESS_KEY` | `amazon-bedrock` |
| `AZURE_API_KEY` | `azure` |
| `AZURE_COGNITIVE_SERVICES_API_KEY` | `azure-cognitive-services` |
| `AZURE_COGNITIVE_SERVICES_RESOURCE_NAME` | `azure-cognitive-services` |
| `AZURE_RESOURCE_NAME` | `azure` |
| `BAILING_API_TOKEN` | `bailing` |
| `BASETEN_API_KEY` | `baseten` |
| `BEE_API_KEY` | `bee` |
| `BERGET_API_KEY` | `berget` |
| `BLUECLAW_API_KEY` | `blueclaw` |
| `BOTHUB_API_KEY` | `bothub` |
| `BOUNDLESS_API_KEY` | `boundless` |
| `BUMO_API_KEY` | `bumo` |
| `BYTEPLUS_API_KEY` | `byteplus` |
| `CEDARLLM_API_KEY` | `cedarllm` |
| `CEREBRAS_API_KEY` | `cerebras` |
| `CHEAPAIMODEL_API_KEY` | `cheapaimodel` |
| `CHEAPEST_TOKEN_API_KEY` | `cheapest-token` |
| `CHEAPLLM_API_KEY` | `cheapllm` |
| `CHUTES_API_KEY` | `chutes` |
| `CLARIFAI_PAT` | `clarifai` |
| `CLAUDE_CODE_OAUTH_TOKEN` | `claude-code` |
| `CLAUDINIO_API_KEY` | `claudinio` |
| `CLINE_API_KEY` | `cline-pass` |
| `CLOUDCODE_API_KEY` | `cloudcode` |
| `CLOUDFERRO_SHERLOCK_API_KEY` | `cloudferro-sherlock` |
| `CLOUDFLARE_ACCOUNT_ID` | `cloudflare-ai-gateway`, `cloudflare-workers-ai` |
| `CLOUDFLARE_API_KEY` | `cloudflare-workers-ai` |
| `CLOUDFLARE_API_TOKEN` | `cloudflare-ai-gateway` |
| `CLOUDFLARE_GATEWAY_ID` | `cloudflare-ai-gateway` |
| `CMD_API_KEY` | `commandcode` |
| `CODEX_API_KEY` | `codex` |
| `COHERE_API_KEY` | `cohere` |
| `CONFIDENTIAL_API_KEY` | `confidential-ai` |
| `COPILOT_GITHUB_TOKEN` | `copilot-cli` |
| `CORAL_API_KEY` | `coralbricks` |
| `CORTECS_API_KEY` | `cortecs` |
| `CROSSMODEL_API_KEY` | `crossmodel` |
| `CRUSOE_API_KEY` | `crusoe` |
| `CURSOR_API_KEY` | `cursor` |
| `DAOXE_API_KEY` | `daoxe` |
| `DASHSCOPE_API_KEY` | `alibaba`, `alibaba-cn` |
| `DATABRICKS_HOST` | `databricks` |
| `DATABRICKS_TOKEN` | `databricks` |
| `DEEPINFRA_API_KEY` | `deepinfra` |
| `DEEPSEEK_API_KEY` | `deepseek` |
| `DIGITALOCEAN_ACCESS_TOKEN` | `digitalocean` |
| `DINFERENCE_API_KEY` | `dinference` |
| `DOTHOOSH_API_KEY` | `dothoosh` |
| `DOUBLEWORD_API_KEY` | `doubleword` |
| `DRUN_API_KEY` | `drun` |
| `EBCLOUD_API_KEY` | `ebcloud` |
| `ECHO_API_KEY` | `echo` |
| `EDENAI_API_KEY` | `edenai` |
| `EMPIRIOLABS_API_KEY` | `empiriolabs` |
| `ENGY_API_KEY` | `engy` |
| `ENTRIM_API_KEY` | `entrim` |
| `EVROC_API_KEY` | `evroc` |
| `EZKIELYNA_API_KEY` | `ezkielyna` |
| `FASTROUTER_API_KEY` | `fastrouter` |
| `FIFTYX_API_KEY` | `fiftyx` |
| `FIREWORKS_API_KEY` | `fireworks-ai` |
| `FORTYTWO_API_KEY` | `fortytwo` |
| `FREEMODEL_API_KEY` | `freemodel` |
| `FREQUENCY_API_KEY` | `frequency` |
| `FRIENDLI_TOKEN` | `friendli` |
| `FROGBOT_API_KEY` | `frogbot` |
| `GEMINI_API_KEY` | `google` |
| `GISOO_API_KEY` | `gisoo` |
| `GITHUB_TOKEN` | `github-copilot` |
| `GITLAB_TOKEN` | `gitlab` |
| `GMICLOUD_API_KEY` | `gmicloud` |
| `GOOGLE_API_KEY` | `google` |
| `GOOGLE_APPLICATION_CREDENTIALS` | `google-vertex`, `google-vertex-anthropic` |
| `GOOGLE_GENERATIVE_AI_API_KEY` | `google` |
| `GOOGLE_VERTEX_LOCATION` | `google-vertex`, `google-vertex-anthropic` |
| `GOOGLE_VERTEX_PROJECT` | `google-vertex`, `google-vertex-anthropic` |
| `GRAPHN_API_KEY` | `graphn` |
| `GREENPT_API_KEY` | `greenpt` |
| `GROQ_API_KEY` | `groq` |
| `HELICONE_API_KEY` | `helicone` |
| `HETZNER_API_KEY` | `hetzner` |
| `HF_TOKEN` | `huggingface` |
| `HOPPER_API_KEY` | `hopper` |
| `HOSTINGER_ROUTER_API_KEY` | `hostinger-router` |
| `HPC_AI_API_KEY` | `hpc-ai` |
| `HUBRIS_API_KEY` | `hubris` |
| `HUMAIN_API_KEY` | `humain` |
| `HYPER_API_KEY` | `hyper` |
| `IMPOSSIBL_API_KEY` | `impossibl` |
| `INCEPTION_API_KEY` | `inception` |
| `INCEPTRON_API_KEY` | `inceptron` |
| `INCO_API_KEY` | `inco` |
| `INFERALL_API_KEY` | `inferall` |
| `INFERENCEAPIS_API_KEY` | `inferenceapis` |
| `INFERENCE_API_KEY` | `inference` |
| `INFERENCE_COOP_API_KEY` | `inference-coop` |
| `INFERRIS_API_KEY` | `inferris` |
| `INFERSIA_API_KEY` | `infersia` |
| `INFERX_API_KEY` | `inferx` |
| `INFER_API_KEY` | `infer` |
| `INFOMANIAK_API_KEY` | `infomaniak` |
| `INFOMANIAK_PRODUCT_ID` | `infomaniak` |
| `IOINTELLIGENCE_API_KEY` | `io-net` |
| `ISOQUANT_API_KEY` | `isoquant` |
| `ITERACOMPUTE_API_KEY` | `iteracompute` |
| `JAD_API_KEY` | `jad-ai` |
| `JALAPENO_API_KEY` | `jalapeno` |
| `JIEKOU_API_KEY` | `jiekou` |
| `JOINGONKA_API_KEY` | `joingonka` |
| `JZSTOKEN_API_KEY` | `jzstoken` |
| `KELN_API_KEY` | `keln` |
| `KENARI_API_KEY` | `kenari` |
| `KILO_API_KEY` | `kilo` |
| `KIMI_API_KEY` | `kimi-code-plan-cn`, `kimi-code-plan-global` |
| `KITANI_API_KEY` | `kitani` |
| `KLAVO_API_KEY` | `klavo` |
| `KLOKINTEGRATION_API_KEY` | `klokintegration` |
| `KOROUTER_API_KEY` | `korouter` |
| `KOSMIK_API_KEY` | `kosmik` |
| `KUAE_API_KEY` | `kuae-cloud-coding-plan` |
| `KUNAVO_API_KEY` | `kunavo` |
| `LIQUID_API_KEY` | `liquid` |
| `LITHOSAI_API_KEY` | `lithosai` |
| `LLMGATEWAY_API_KEY` | `llmgateway`, `llmgateway-providers` |
| `LLMTECH_API_KEY` | `llmtech` |
| `LLMTR_API_KEY` | `llmtr` |
| `LMSTUDIO_API_KEY` | `lmstudio` |
| `LOGITS_API_KEY` | `logits` |
| `LONGCAT_API_KEY` | `longcat` |
| `LOWROUTER_API_KEY` | `lowrouter` |
| `LUCIDQUERY_API_KEY` | `lucidquery` |
| `LYCEUM_API_KEY` | `lyceum` |
| `LYNKR_API_KEY` | `lynkr` |
| `MAAS_API_KEY` | `huawei-maas` |
| `MALLARD_API_KEY` | `mallard` |
| `MEGANOVA_API_KEY` | `meganova` |
| `MELIOUS_API_KEY` | `melious` |
| `MERAGPT_API_KEY` | `meragpt` |
| `MERGE_GATEWAY_API_KEY` | `merge-gateway` |
| `META_MODEL_API_KEY` | `meta` |
| `MINIMAX_API_KEY` | `minimax`, `minimax-cn`, `minimax-cn-coding-plan`, `minimax-coding-plan` |
| `MISTRAL_API_KEY` | `mistral` |
| `MIXLAYER_API_KEY` | `mixlayer` |
| `MOARK_API_KEY` | `moark` |
| `MODAL_PROXY_TOKEN` | `modal` |
| `MODELIS_API_KEY` | `modelis` |
| `MODELSCOPE_API_KEY` | `modelscope` |
| `MODEL_ORACLE_API_KEY` | `model-oracle-ai` |
| `MODULAR_API_KEY` | `modular` |
| `MONEO_API_KEY` | `moneo` |
| `MOONSHOT_API_KEY` | `moonshotai`, `moonshotai-cn` |
| `MORPH_API_KEY` | `morph` |
| `MUNITO_API_KEY` | `munito`, `munito-eu` |
| `NANO_GPT_API_KEY` | `nano-gpt` |
| `NAN_API_KEY` | `nan` |
| `NEARAI_API_KEY` | `nearai` |
| `NEBIUS_API_KEY` | `nebius` |
| `NEON_AI_GATEWAY_BASE_URL` | `neon` |
| `NEON_AI_GATEWAY_TOKEN` | `neon` |
| `NEOSMITH_API_KEY` | `neosmith` |
| `NEURALWATT_API_KEY` | `neuralwatt` |
| `NODECODE_API_KEY` | `nodecode` |
| `NOKEEP_API_KEY` | `nokeep` |
| `NOVA_API_KEY` | `nova` |
| `NOVITA_API_KEY` | `novita-ai` |
| `NVIDIA_API_KEY` | `nvidia` |
| `OATHBREAKER_API_KEY` | `oathbreaker` |
| `OBSCURIFY_API_KEY` | `obscurify` |
| `OCI_GENAI_API_KEY` | `oci` |
| `OFOX_API_KEY` | `ofox` |
| `OLLAMA_API_KEY` | `ollama-cloud` |
| `OPENAI_API_KEY` | `openai` |
| `OPENCODE_API_KEY` | `opencode`, `opencode-go` |
| `OPENREASON_API_KEY` | `openreason` |
| `OPENROUTER_API_KEY` | `openrouter` |
| `OPPER_API_KEY` | `opper` |
| `ORCAROUTER_API_KEY` | `orcarouter` |
| `OUTERLABS_API_KEY` | `outerlabs` |
| `OVHCLOUD_API_KEY` | `ovhcloud` |
| `PARALONCLOUD_API_KEY` | `paraloncloud` |
| `PARETO_API_KEY` | `pareto` |
| `PENDRA_API_KEY` | `pendra` |
| `PERPLEXITY_API_KEY` | `perplexity`, `perplexity-agent` |
| `PHALA_AI_API_KEY` | `phala` |
| `PHASE_API_KEY` | `phase` |
| `PIONEER_API_KEY` | `pioneer` |
| `POE_API_KEY` | `poe` |
| `POOLSIDE_API_KEY` | `poolside` |
| `POWERSPRING_API_KEY` | `powerspring` |
| `PRIME_API_KEY` | `prime` |
| `PRIVATEMODE_API_KEY` | `privatemode-ai` |
| `QIHANG_API_KEY` | `qihang-ai` |
| `QINIU_API_KEY` | `qiniu-ai` |
| `QVAC_API_KEY` | `qvac` |
| `RAYLINE_API_KEY` | `rayline` |
| `REFLECTION_API_KEY` | `reflection` |
| `REFUSELESS_API_KEY` | `refuseless` |
| `REGOLO_API_KEY` | `regolo-ai` |
| `RELAY_API_KEY` | `proxystacks` |
| `REQUESTY_API_KEY` | `requesty` |
| `ROUTING_RUN_API_KEY` | `routing-run` |
| `RUAPI_API_KEY` | `ruapi` |
| `RUNCRATE_API_KEY` | `runcrate` |
| `RUNINFRA_GATEWAY_KEY` | `runinfra` |
| `SAGG_API_KEY` | `sagg` |
| `SAKANA_API_KEY` | `sakana` |
| `SALAD_CLOUD_API_KEY` | `salad-cloud` |
| `SAMBANOVA_API_KEY` | `sambanova` |
| `SANDBASE_API_KEY` | `sandbase` |
| `SARVAM_API_KEY` | `sarvam` |
| `SCALEWAY_API_KEY` | `scaleway` |
| `SCNET_API_KEY` | `scnet-token-plan` |
| `SCX_API_KEY` | `scx-ai` |
| `SENSENOVA_API_KEY` | `sensenova` |
| `SILICONFLOW_API_KEY` | `siliconflow` |
| `SILICONFLOW_CN_API_KEY` | `siliconflow-cn` |
| `SNOWFLAKE_ACCOUNT` | `snowflake-cortex` |
| `SNOWFLAKE_CORTEX_PAT` | `snowflake-cortex` |
| `SOKKAN_API_KEY` | `sokkan` |
| `SPARROWCODES_API_KEY` | `sparrowcodes` |
| `STACKIT_API_KEY` | `stackit` |
| `STANDARDCOMPUTE_API_KEY` | `standardcompute` |
| `STEADYROUTER_API_KEY` | `steadyrouter` |
| `STEPFUN_API_KEY` | `stepfun`, `stepfun-ai`, `stepfun-ai-step-plan`, `stepfun-step-plan` |
| `STONEY_API_KEY` | `stepping-stone` |
| `SUBCONSCIOUS_API_KEY` | `subconscious` |
| `SUBMODEL_INSTAGEN_ACCESS_KEY` | `submodel` |
| `SUPERBOT_API_KEY` | `superbot-gg` |
| `SYNTHETIC_API_KEY` | `synthetic` |
| `SYNTHORAI_API_KEY` | `synthorai` |
| `TEAMOROUTER_API_KEY` | `teamorouter` |
| `TEAPOT_ROUTER_API_KEY` | `teapot-router` |
| `TENCENT_CODING_PLAN_API_KEY` | `tencent-coding-plan` |
| `TENCENT_TOKENHUB_API_KEY` | `tencent-tokenhub` |
| `TENCENT_TOKEN_PLAN_API_KEY` | `tencent-token-plan` |
| `TENSORX_API_KEY` | `tensorx` |
| `TENSTORRENT_API_KEY` | `tenstorrent` |
| `THEGRID_API_KEY` | `the-grid-ai` |
| `THEROUTER_API_KEY` | `therouter` |
| `TINFOIL_API_KEY` | `tinfoil` |
| `TINKER_API_KEY` | `thinkingmachines` |
| `TOAPIS_API_KEY` | `toapis` |
| `TOGETHER_API_KEY` | `togetherai` |
| `TOKENGO_API_KEY` | `tokengo` |
| `TOKENIFY_API_KEY` | `tokenify` |
| `TOKENLAB_API_KEY` | `tokenlab` |
| `TOKENROUTER_API_KEY` | `tokenrouter` |
| `TOKENWATT_API_KEY` | `tokenwatt` |
| `TRUSTEDROUTER_API_KEY` | `trustedrouter` |
| `TRUSTEDTOKENS_API_KEY` | `trustedtokens` |
| `UMANS_AI_API_KEY` | `umans-ai` |
| `UMANS_AI_CODING_PLAN_API_KEY` | `umans-ai-coding-plan` |
| `UNBIASED_API_KEY` | `unbiased` |
| `UNCENSURED_API_KEY` | `uncensuredai` |
| `UNOROUTER_API_KEY` | `unorouter` |
| `UNRESTRICTED_API_KEY` | `unrestricted` |
| `UPLINKSWITCH_API_KEY` | `uplinkswitch` |
| `UPSTAGE_API_KEY` | `upstage` |
| `VANCINE_API_KEY` | `vancine` |
| `VENICE_API_KEY` | `venice` |
| `VERDA_INFERENCE_API_KEY` | `verda` |
| `VISPARK_LAB_API_KEY` | `vispark` |
| `VIVGRID_API_KEY` | `vivgrid` |
| `VULTR_API_KEY` | `vultr` |
| `WAFER_API_KEY` | `wafer.ai` |
| `WALLABY_API_KEY` | `wallaby` |
| `WALLY_API_KEY` | `wally` |
| `WANDB_API_KEY` | `wandb` |
| `WATSONX_AI_APIKEY` | `watsonx` |
| `WATSONX_AI_PROJECT_ID` | `watsonx` |
| `WATT_API_KEY` | `watt` |
| `XAI_API_KEY` | `xai` |
| `XIAOMI_API_KEY` | `xiaomi`, `xiaomi-token-plan-ams`, `xiaomi-token-plan-cn`, `xiaomi-token-plan-sgp` |
| `XPERSONA_API_KEY` | `xpersona` |
| `XXAPI_API_KEY` | `xxapi` |
| `ZELDOC_API_KEY` | `zeldoc` |
| `ZENIFRA_AI_API_KEY` | `zenifra` |
| `ZENIFRA_AI_KEY` | `zenifra` |
| `ZENMUX_API_KEY` | `zenmux` |
| `ZEROGPU_API_KEY` | `zerogpu` |
| `ZHIPU_API_KEY` | `zai`, `zai-coding-plan`, `zhipuai`, `zhipuai-coding-plan` |
| `ZRO_API_KEY` | `zro` |
