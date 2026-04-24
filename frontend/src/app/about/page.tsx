export default function AboutPage() {
    return (
      <div className="max-w-3xl mx-auto px-4 py-16">
        <h1 className="text-4xl font-bold mb-6">About SnowView</h1>
  
        {/* Personal Story */}
        <section className="mb-12">
          <p className="text-slate-300 leading-relaxed mb-4">
            SnowView was born out of a simple frustration: before every snowboard trip,
            I found myself checking five different websites to piece together whether
            conditions were actually worth the drive. SNOTEL data on one site, forecasts
            on another, resort reports that are always optimistic — none of it in one place,
            and none of it scored in a way that helps you make a decision.
          </p>
          <p className="text-slate-300 leading-relaxed">
            So I built the tool I wanted. SnowView aggregates real data from federal
            monitoring stations and weather services, runs it through a scoring model,
            and gives you one number per resort. No marketing spin — just data.
          </p>
        </section>
  
        {/* How It Works */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">How It Works</h2>
  
          <div className="space-y-6">
            <Step
              number="1"
              title="Data Ingestion"
              description="A Python ETL pipeline pulls daily observations from ~230 SNOTEL stations across California, Colorado, and Washington via the USDA NRCS AWDB API. It also ingests 7-day forecasts from the NOAA National Weather Service API for each resort location."
            />
            <Step
              number="2"
              title="Spatial Processing"
              description="Each resort is linked to its nearest SNOTEL stations using PostGIS spatial queries, weighted by both distance and elevation similarity. Raw station data is transformed into resort-level metrics through weighted averages."
            />
            <Step
              number="3"
              title="Condition Scoring"
              description="A composite scoring model (0-100) evaluates each resort based on recent snowfall (30%), snow depth (25%), forecast outlook (20%), snowpack trend (15%), and temperature (10%). Each score includes a plain-language explanation of what's driving it."
            />
            <Step
              number="4"
              title="Delivery"
              description="Results are served through a FastAPI backend and exported as GeoJSON for the map. The frontend uses ArcGIS Maps SDK for JavaScript to render an interactive map with score-coded markers, detail panels, and resort comparison tools."
            />
          </div>
        </section>
  
        {/* Data Sources */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Data Sources</h2>
          <div className="grid md:grid-cols-2 gap-4">
            <SourceCard
              name="SNOTEL"
              org="USDA Natural Resources Conservation Service"
              description="Snow depth, snow water equivalent, precipitation, and temperature from ~800 automated stations across the western US."
              url="https://www.nrcs.usda.gov/wps/portal/wcc/home/snowClimateMonitoring/snowpack/"
            />
            <SourceCard
              name="National Weather Service"
              org="NOAA"
              description="Gridded weather forecasts including projected snowfall, temperature, and wind speed, updated multiple times daily."
              url="https://www.weather.gov/documentation/services-web-api"
            />
          </div>
        </section>
  
        {/* Tech Stack */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold mb-4">Tech Stack</h2>
          <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-5">
            <div className="grid md:grid-cols-2 gap-4 text-sm">
              <div>
                <p className="text-slate-400 mb-1">Frontend</p>
                <p className="text-white">Next.js · TypeScript · Tailwind CSS · ArcGIS Maps SDK for JavaScript</p>
              </div>
              <div>
                <p className="text-slate-400 mb-1">Backend</p>
                <p className="text-white">FastAPI · Python · PostGIS · PostgreSQL</p>
              </div>
              <div>
                <p className="text-slate-400 mb-1">Data Pipeline</p>
                <p className="text-white">SNOTEL AWDB API · NOAA NWS API · Scheduled ETL · GeoJSON Export</p>
              </div>
              <div>
                <p className="text-slate-400 mb-1">Infrastructure</p>
                <p className="text-white">AWS EC2 · Vercel · ArcGIS Online Feature Service</p>
              </div>
            </div>
          </div>
        </section>
  
        {/* Coverage */}
        <section>
          <h2 className="text-2xl font-bold mb-4">Coverage</h2>
          <p className="text-slate-300 mb-4">
            SnowView currently tracks 30 resorts across three states, with data from
            232 SNOTEL monitoring stations. Coverage depends on SNOTEL station
            availability — some areas (like Southern California) have limited monitoring
            infrastructure.
          </p>
          <div className="grid grid-cols-3 gap-4 text-center">
            <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-4">
              <p className="text-2xl font-bold text-blue-400">10</p>
              <p className="text-sm text-slate-400">California</p>
            </div>
            <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-4">
              <p className="text-2xl font-bold text-blue-400">12</p>
              <p className="text-sm text-slate-400">Colorado</p>
            </div>
            <div className="bg-slate-900/50 border border-slate-800 rounded-xl p-4">
              <p className="text-2xl font-bold text-blue-400">8</p>
              <p className="text-sm text-slate-400">Washington</p>
            </div>
          </div>
        </section>
      </div>
    );
  }
  
  function Step({
    number,
    title,
    description,
  }: {
    number: string;
    title: string;
    description: string;
  }) {
    return (
      <div className="flex gap-4">
        <div className="flex-shrink-0 w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center text-sm font-bold text-white">
          {number}
        </div>
        <div>
          <h3 className="font-semibold text-white mb-1">{title}</h3>
          <p className="text-sm text-slate-400 leading-relaxed">{description}</p>
        </div>
      </div>
    );
  }
  
  function SourceCard({
    name,
    org,
    description,
    url,
  }: {
    name: string;
    org: string;
    description: string;
    url: string;
  }) {
    return (
      <a
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        className="block bg-slate-800/50 border border-slate-700/50 rounded-xl p-4 hover:border-blue-500/50 transition-colors"
      >
        <p className="font-semibold text-white">{name}</p>
        <p className="text-xs text-slate-500 mb-2">{org}</p>
        <p className="text-sm text-slate-400">{description}</p>
      </a>
    );
  }