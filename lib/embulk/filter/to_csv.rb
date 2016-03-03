Embulk::JavaPlugin.register_filter(
  "to_csv", "org.embulk.filter.to_csv.ToCsvFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
