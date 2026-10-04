// The app logo, imported through the bundler so its file name carries a content hash: a new logo can never be hidden
// by a cached old copy in the window (WebView2 cache). `scripts/make_icons.py` writes src/assets/app-logo.png.
import logo from "./assets/app-logo.png";

export const APP_LOGO: string = logo;
