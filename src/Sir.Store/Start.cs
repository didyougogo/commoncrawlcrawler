﻿using Microsoft.Extensions.DependencyInjection;

namespace Sir.Store
{
    /// <summary>
    /// Initialize app.
    /// </summary>
    public class Start : IPluginStart
    {
        public void OnApplicationStartup(IServiceCollection services, ServiceProvider serviceProvider)
        {
            var tokenizer = new LatinTokenizer();
            var config = serviceProvider.GetService<IConfigurationProvider>();

            services.AddSingleton(typeof(SessionFactory), 
                new SessionFactory(
                    config.Get("data_dir"), 
                    tokenizer,
                    config));

            services.AddSingleton(typeof(ITokenizer), tokenizer);

            services.AddSingleton(typeof(HttpQueryParser), new HttpQueryParser(new TermQueryParser(), tokenizer));
            services.AddSingleton(typeof(HttpBowQueryParser), new HttpBowQueryParser(tokenizer));
            services.AddSingleton(typeof(IQueryFormatter), new QueryFormatter());
        }
    }
}