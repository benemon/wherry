import type { Forest, Model, RunDetail, Transcript } from './types';

export class ApiError extends Error {
  status: number;

  constructor(status: number) {
    super('HTTP ' + status);
    this.status = status;
  }
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, { credentials: 'same-origin', ...init });
  if (!response.ok) {
    throw new ApiError(response.status);
  }
  return (response.status === 204 ? null : await response.json()) as T;
}

export const getModel = (): Promise<Model> => api('/api/model');

export const getRun = (id: string): Promise<RunDetail> => api('/api/runs/' + encodeURIComponent(id));

export const getTrace = (id: string): Promise<Forest> => api('/api/runs/' + encodeURIComponent(id) + '/trace');

export const getTranscript = (id: string, query: URLSearchParams): Promise<Transcript> =>
  api('/api/runs/' + encodeURIComponent(id) + '/transcript?' + query.toString());

// session posts the token from the fragment once for the session cookie.
export const postSession = (token: string): Promise<null> => {
  const body = new URLSearchParams();
  body.set('token', token);
  return api('/session', { method: 'POST', body });
};
