import {
  createModels,
  createProvider,
  envApiKeyAuth,
} from "@earendil-works/pi-ai";
import { openAICompletionsApi } from "@earendil-works/pi-ai/api/openai-completions.lazy";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";

const MAX_PROMPT_CHARS = 64_000;

function output(value, exitCode = 0) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
  process.exitCode = exitCode;
}

function safeErrorKind(error) {
  return error?.errorKind || "model_error";
}

function safeErrorMessage(kind) {
  const messages = {
    invalid_input: "pi-ai helper input invalid",
    provider_missing: "pi-ai provider or credential unavailable",
    timeout: "pi-ai provider timeout",
    invalid_output: "pi-ai provider output invalid",
    helper_error: "pi-ai helper failed",
    model_error: "pi-ai model request failed",
  };
  return messages[kind] || messages.model_error;
}

function isNonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function validateRequest(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("request 必须是 JSON object");
  }
  if (!isNonEmptyString(value.provider) || !isNonEmptyString(value.model)) {
    throw new Error("provider/model 必须是非空字符串");
  }
  if (!isNonEmptyString(value.prompt) || value.prompt.length > MAX_PROMPT_CHARS) {
    throw new Error("prompt 为空或超过 helper 限制");
  }
  if (value.base_url !== "" && !isNonEmptyString(value.base_url)) {
    throw new Error("base_url 必须是字符串");
  }
  if (value.api_key_env !== "" && !isNonEmptyString(value.api_key_env)) {
    throw new Error("api_key_env 必须是字符串");
  }
  if (!Number.isInteger(value.timeout_ms) || value.timeout_ms < 100 || value.timeout_ms > 300_000) {
    throw new Error("timeout_ms 超出范围");
  }
  return value;
}

function customModel(request) {
  const model = {
    id: request.model,
    name: request.model,
    api: "openai-completions",
    provider: "custom",
    baseUrl: request.base_url,
    reasoning: false,
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: 128_000,
    maxTokens: 32,
  };
  const models = createModels();
  models.setProvider(
    createProvider({
      id: "custom",
      name: "OneBot11 custom OpenAI-compatible provider",
      auth: {
        apiKey: envApiKeyAuth("OneBot11 trigger API key", [request.api_key_env]),
      },
      models: [model],
      api: openAICompletionsApi(),
    }),
  );
  return { models, model };
}

function builtinModel(request) {
  const models = builtinModels();
  const model = models.getModel(request.provider, request.model);
  if (!model) {
    throw Object.assign(
      new Error(`provider/model 不存在: ${request.provider}/${request.model}`),
      { errorKind: "provider_missing" },
    );
  }
  return { models, model };
}

async function run(request) {
  const key = request.api_key_env ? process.env[request.api_key_env] : undefined;
  if (request.api_key_env && !key) {
    throw Object.assign(new Error(`缺少环境变量: ${request.api_key_env}`), {
      errorKind: "provider_missing",
    });
  }
  const selected = request.provider === "custom"
    ? customModel(request)
    : builtinModel(request);
  const response = await selected.models.completeSimple(
    selected.model,
    {
      systemPrompt: "你是严格的 OneBot11 消息触发判断器。只能返回 JSON，不要输出 Markdown。",
      messages: [
        { role: "user", content: request.prompt, timestamp: Date.now() },
      ],
    },
    {
      temperature: 0,
      maxTokens: 32,
      timeoutMs: request.timeout_ms,
      maxRetries: 0,
      apiKey: key,
    },
  );
  if (response.stopReason === "error" || response.errorMessage) {
    throw Object.assign(
      new Error(response.errorMessage || "provider 返回错误"),
      { errorKind: "model_error" },
    );
  }
  const text = response.content
    .filter((block) => block && block.type === "text")
    .map((block) => block.text)
    .join("")
    .trim();
  if (!text) {
    throw Object.assign(new Error("provider 没有返回文本"), {
      errorKind: "invalid_output",
    });
  }
  return text;
}

let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  input += chunk;
});
process.stdin.on("end", async () => {
  try {
    const request = validateRequest(JSON.parse(input));
    const text = await run(request);
    output({ ok: true, text });
  } catch (error) {
    const errorKind = safeErrorKind(error);
    output(
      {
        ok: false,
        error_kind: errorKind,
        error: safeErrorMessage(errorKind),
      },
      1,
    );
  }
});
