﻿using Microsoft.Extensions.DependencyInjection;
using Sir.HttpServer.Features;
using System;
using System.IO;
using System.Linq;
using System.Runtime.Loader;

namespace Sir.HttpServer
{
    public static class ServiceConfiguration
    {
        public static void RegisterComponents(
            IServiceCollection services, PluginsCollection plugins, IServiceProvider container)
        {
            services.AddSingleton(typeof(JobQueue));
            services.AddSingleton(typeof(SaveAsJobQueue));
        }

        public static IServiceProvider Configure(IServiceCollection services)
        {
            var assemblyPath = Directory.GetCurrentDirectory();
            var config = new KeyValueConfiguration(Path.Combine(assemblyPath, "sir.ini"));

            // register config
            services.Add(new ServiceDescriptor(typeof(IConfigurationProvider), config));

            // register plugin startup and teardown handlers

#if DEBUG
            var frameworkVersion = AppContext.TargetFrameworkName.Substring(AppContext.TargetFrameworkName.IndexOf("=v") + 2);

            assemblyPath = Path.Combine(assemblyPath, "bin", "Debug", $"netcoreapp{frameworkVersion}");
#endif

            var files = Directory.GetFiles(assemblyPath, "*.plugin.dll");

            foreach (var assembly in files.Select(file => AssemblyLoadContext.Default.LoadFromAssemblyPath(file)))
            {
                foreach (var type in assembly.GetTypes())
                {
                    // search for concrete types
                    if (!type.IsInterface)
                    {
                        var interfaces = type.GetInterfaces();

                        if (interfaces.Contains(typeof(IPluginStop)))
                        {
                            services.Add(new ServiceDescriptor(typeof(IPluginStop), type, ServiceLifetime.Singleton));
                        }
                        else if (interfaces.Contains(typeof(IPluginStart)))
                        {
                            services.Add(new ServiceDescriptor(typeof(IPluginStart), type, ServiceLifetime.Singleton));
                        }
                    }
                }
            }

            var plugins = new PluginsCollection();

            services.Add(new ServiceDescriptor(typeof(PluginsCollection), plugins));

            var serviceProvider = services.BuildServiceProvider();

            // raise startup event
            foreach (var service in serviceProvider.GetServices<IPluginStart>())
            {
                service.OnApplicationStartup(services, serviceProvider, config);
            }

            // Fetch one instance each of all plugins and register them with the PluginCollection
            // so that they can be fetched at runtime by Content-Type and System.Type.

            foreach (var service in services.BuildServiceProvider().GetServices<IHttpWriter>())
            {
                plugins.Add(service.ContentType, service);
            }

            foreach (var service in services.BuildServiceProvider().GetServices<IHttpReader>())
            {
                plugins.Add(service.ContentType, service);
            }

            RegisterComponents(services, plugins, services.BuildServiceProvider());

            return services.BuildServiceProvider();
        }
    }
}
