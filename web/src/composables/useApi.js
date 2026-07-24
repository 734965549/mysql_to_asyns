export const API_BASE = "/api";

export async function handleApiError(response, defaultMsg = "操作失败") {
  try {
    const errData = await response.json();

    if (errData.error) {
      const errorMsg = errData.error;
      if (errorMsg.includes(":")) {
        return `${defaultMsg}: ${errorMsg}`;
      }
      return `${defaultMsg}: ${errorMsg}`;
    }

    return defaultMsg;
  } catch (e) {
    return defaultMsg;
  }
}

export function useApi() {
  return { API_BASE, handleApiError };
}
