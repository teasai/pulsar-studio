#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lang {
    Zh,
    ZhHant,
    En,
}

impl Default for Lang {
    fn default() -> Self {
        Lang::Zh
    }
}

impl Lang {
    pub fn from_code(s: &str) -> Self {
        match s {
            "en" => Lang::En,
            "zh-hant" => Lang::ZhHant,
            _ => Lang::Zh,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Lang::Zh => "zh",
            Lang::ZhHant => "zh-hant",
            Lang::En => "en",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Lang::Zh => "简体中文",
            Lang::ZhHant => "繁體中文",
            Lang::En => "English",
        }
    }
}

/// 三语取词：`t(lang, "简体", "繁體", "English")`
pub fn t(lang: Lang, zh: &'static str, hant: &'static str, en: &'static str) -> &'static str {
    match lang {
        Lang::Zh => zh,
        Lang::ZhHant => hant,
        Lang::En => en,
    }
}
