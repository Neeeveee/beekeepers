import {Composition} from "remotion";
import {DashboardLaunch} from "./DashboardLaunch";

export const RemotionRoot = () => {
  return (
    <Composition
      id="DashboardLaunch"
      component={DashboardLaunch}
      durationInFrames={480}
      fps={30}
      width={1920}
      height={1080}
    />
  );
};
