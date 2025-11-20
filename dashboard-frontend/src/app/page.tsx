'use client';

import { useState, useEffect, FormEvent } from 'react';

// Define the structure of the config object for TypeScript
interface MinioConfig {
  enabled?: boolean;
  endpoint?: string;
  bucket_name?: string;
  secure?: boolean;
}

interface CrawlerConfig {
  start_urls?: string[];
  max_depth?: number;
  max_pages?: number;
  save_to_db?: boolean;
  minio_storage?: MinioConfig;
  [key: string]: any; // Allow other properties
}

export default function DashboardPage() {
  const [config, setConfig] = useState<CrawlerConfig | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  // Fetch config from the backend API
  useEffect(() => {
    if (!apiBaseUrl) {
      setError("API base URL is not configured.");
      setIsLoading(false);
      return;
    }

    fetch(`${apiBaseUrl}/api/config`)
      .then(res => {
        if (!res.ok) {
          throw new Error('Failed to fetch config. Please ensure the backend is running.');
        }
        return res.json();
      })
      .then((data: CrawlerConfig) => {
        setConfig(data);
        setIsLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setIsLoading(false);
      });
  }, [apiBaseUrl]);

  // Handle form input changes
  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value, type } = e.target;

    if (config === null) return;

    // Handle nested minio_storage fields
    if (name.startsWith('minio_')) {
      const minioKey = name.split('_')[1] as keyof MinioConfig;
      const isCheckbox = type === 'checkbox';
      const checkedValue = (e.target as HTMLInputElement).checked;

      setConfig({
        ...config,
        minio_storage: {
          ...config.minio_storage,
          [minioKey]: isCheckbox ? checkedValue : value,
        },
      });
      return;
    }

    // Handle regular fields
    const isCheckbox = type === 'checkbox';
    const checkedValue = (e.target as HTMLInputElement).checked;

    let processedValue: any = value;
    if (type === 'number') {
        processedValue = value === '' ? 0 : parseInt(value, 10);
    } else if (name === 'start_urls') {
        processedValue = value.split('\\n');
    } else if (isCheckbox) {
        processedValue = checkedValue;
    }

    setConfig({
      ...config,
      [name]: processedValue,
    });
  };

  // Handle form submission
  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setSuccessMessage(null);

    if (!config) {
      setError("Configuration data is not loaded.");
      return;
    }
    if (!apiBaseUrl) {
        setError("API base URL is not configured.");
        return;
    }

    try {
      const response = await fetch(`${apiBaseUrl}/api/config`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
      });

      if (!response.ok) {
        throw new Error('Failed to save config. The server returned an error.');
      }

      setSuccessMessage('Configuration saved successfully!');
      // Hide the message after 3 seconds
      setTimeout(() => setSuccessMessage(null), 3000);

    } catch (err: any) {
      setError(err.message);
    }
  };

  if (isLoading) return <div className="flex min-h-screen items-center justify-center bg-gray-100"><p>Loading configuration...</p></div>;
  if (error) return <div className="flex min-h-screen items-center justify-center bg-gray-100"><p className="text-red-500">Error: {error}</p></div>;
  if (!config) return <div className="flex min-h-screen items-center justify-center bg-gray-100"><p>No configuration data found.</p></div>;

  return (
    <main className="flex min-h-screen flex-col items-center justify-center bg-gray-50 p-4 sm:p-8">
      <div className="w-full max-w-2xl rounded-lg bg-white p-6 shadow-md sm:p-8">
        <h1 className="mb-6 text-2xl font-bold text-gray-800">Crawler Control Panel</h1>

        {successMessage && <div className="mb-4 rounded-md bg-green-100 p-3 text-center text-green-700">{successMessage}</div>}

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Start URLs */}
          <div>
            <label htmlFor="start_urls" className="block text-sm font-medium text-gray-700">
              Start URLs (one per line)
            </label>
            <textarea
              id="start_urls"
              name="start_urls"
              rows={4}
              value={(config.start_urls || []).join('\\n')}
              onChange={handleInputChange}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            />
          </div>

          <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
              {/* Max Depth */}
              <div>
                <label htmlFor="max_depth" className="block text-sm font-medium text-gray-700">
                  Max Depth
                </label>
                <input
                  type="number"
                  id="max_depth"
                  name="max_depth"
                  value={config.max_depth || 0}
                  onChange={handleInputChange}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
                />
              </div>

              {/* Max Pages */}
              <div>
                <label htmlFor="max_pages" className="block text-sm font-medium text-gray-700">
                  Max Pages
                </label>
                <input
                  type="number"
                  id="max_pages"
                  name="max_pages"
                  value={config.max_pages || 0}
                  onChange={handleInputChange}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
                />
              </div>
          </div>

          {/* Save to DB */}
          <div className="flex items-center">
            <input
              id="save_to_db"
              name="save_to_db"
              type="checkbox"
              checked={config.save_to_db || false}
              onChange={handleInputChange}
              className="h-4 w-4 rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
            />
            <label htmlFor="save_to_db" className="ml-2 block text-sm text-gray-900">
              Save to Database
            </label>
          </div>

          {/* MinIO Section */}
          <fieldset className="rounded-md border border-gray-300 p-4">
            <legend className="px-2 text-sm font-medium text-gray-700">MinIO Storage</legend>
            <div className="space-y-4">
              <div className="flex items-center">
                <input
                  id="minio_enabled"
                  name="minio_enabled"
                  type="checkbox"
                  checked={config.minio_storage?.enabled || false}
                  onChange={handleInputChange}
                  className="h-4 w-4 rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                />
                <label htmlFor="minio_enabled" className="ml-2 block text-sm text-gray-900">
                  Enable MinIO
                </label>
              </div>
              {config.minio_storage?.enabled && (
                <div>
                  <label htmlFor="minio_endpoint" className="block text-sm font-medium text-gray-700">
                    MinIO Endpoint
                  </label>
                  <input
                    type="text"
                    id="minio_endpoint"
                    name="minio_endpoint"
                    value={config.minio_storage?.endpoint || ''}
                    onChange={handleInputChange}
                    placeholder="e.g., minio:9000"
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
                  />
                </div>
              )}
            </div>
          </fieldset>

          {/* Submit Button */}
          <div className="flex justify-end">
            <button
              type="submit"
              className="rounded-md border border-transparent bg-indigo-600 py-2 px-4 text-sm font-medium text-white shadow-sm hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2"
            >
              Save Configuration
            </button>
          </div>
        </form>
      </div>
    </main>
  );
}
