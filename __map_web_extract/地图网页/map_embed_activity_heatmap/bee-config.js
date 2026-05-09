window.BEE_MAP_CONFIG = {
  tileFolder: 'qgis tiles 03',
  points: [
    {
      id: 'S0',
      name: 'S0 基地',
      lat: 30.433993,
      lng: 120.204655,
      activityNormalized: 0.92,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S1',
      name: 'S1',
      lat: 30.426914,
      lng: 120.199533,
      activityNormalized: 0.58,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S2',
      name: 'S2',
      lat: 30.438906,
      lng: 120.215311,
      activityNormalized: 0.64,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S3',
      name: 'S3',
      lat: 30.444617,
      lng: 120.221508,
      activityNormalized: 0.71,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S4',
      name: 'S4',
      lat: 30.433378,
      lng: 120.189569,
      activityNormalized: 0.47,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S5',
      name: 'S5',
      lat: 30.427736,
      lng: 120.219164,
      activityNormalized: 0.82,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S6',
      name: 'S6',
      lat: 30.449753,
      lng: 120.186197,
      activityNormalized: 0.39,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S7',
      name: 'S7',
      lat: 30.438722,
      lng: 120.232653,
      activityNormalized: 0.68,
      time: '2026-04-10 13:00:00'
    },
    {
      id: 'S8',
      name: 'S8',
      lat: 30.408736,
      lng: 120.198575,
      activityNormalized: 0.26,
      time: '2026-04-10 13:00:00'
    }
  ],
  heat: {
    radius: 42,
    blur: 30,
    minOpacity: 0.34,
    maxZoom: 18
  },
  scriptPolling: {
    enabled: true,
    url: './realtime-data.js',
    intervalMs: 10000
  },
  mqtt: {
    enabled: false,
    url: 'wss://h1b01eed.ala.asia-southeast1.emqxsl.com:8084/mqtt',
    username: 'bee01',
    password: '12345678',
    subscriptions: [
      { pointId: 'S0', topic: 'beehive/hive01/10min' },
      { pointId: 'S1', topic: 'beehive/hive02/10min' },
      { pointId: 'S2', topic: 'beehive/hive03/10min' },
      { pointId: 'S3', topic: 'beehive/hive04/10min' },
      { pointId: 'S4', topic: 'beehive/hive05/10min' },
      { pointId: 'S5', topic: 'beehive/hive06/10min' },
      { pointId: 'S6', topic: 'beehive/hive07/10min' },
      { pointId: 'S7', topic: 'beehive/hive08/10min' },
      { pointId: 'S8', topic: 'beehive/hive09/10min' }
    ]
  }
};
